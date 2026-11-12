/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.failover.plugin.fs;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import com.example.failover.plugin.testing.AuditSandbox;
import com.example.failover.plugin.token.HeldToken;
import com.example.failover.plugin.token.MockDelegationTokenReceiver;
import com.example.failover.plugin.token.MockToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenCheckedFileSystemTest {

    @TempDir File tempDir;

    private AuditSandbox audit;
    private TokenCheckedFileSystem fileSystem;

    @BeforeEach
    void setUp() throws IOException {
        audit = AuditSandbox.open(tempDir);
        fileSystem = new TokenCheckedFileSystem(URI.create("tokenfs:///"));
    }

    @AfterEach
    void tearDown() {
        audit.close();
    }

    private Path tokenfsPath(String relative) {
        return new Path("tokenfs", null, new File(tempDir, relative).getPath());
    }

    @Test
    void validTokenAllowsTheWholeLifecycleOfAFileAndReportsTokenfsPaths() throws Exception {
        HeldToken.set(new MockToken("t-1", 0L, Long.MAX_VALUE));
        final Path dir = tokenfsPath("ha");
        final Path file = new Path(dir, "job.bin");

        assertThat(fileSystem.mkdirs(dir)).isTrue();
        try (FSDataOutputStream out = fileSystem.create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write("payload".getBytes(StandardCharsets.UTF_8));
        }

        final FileStatus[] listed = fileSystem.listStatus(dir);
        assertThat(listed).hasSize(1);
        assertThat(listed[0].getPath().toUri().getScheme()).isEqualTo("tokenfs");
        assertThat(listed[0].getPath().getName()).isEqualTo("job.bin");
        assertThat(listed[0].getLen()).isEqualTo("payload".length());
        assertThat(listed[0].isDir()).isFalse();

        // the path reported by the listing is usable for follow-up calls
        try (FSDataInputStream in = fileSystem.open(listed[0].getPath())) {
            final byte[] content = new byte["payload".length()];
            assertThat(in.read(content)).isEqualTo(content.length);
            assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo("payload");
        }
        assertThat(fileSystem.exists(file)).isTrue();

        final Path renamed = new Path(dir, "renamed.bin");
        assertThat(fileSystem.rename(file, renamed)).isTrue();
        assertThat(fileSystem.exists(file)).isFalse();
        assertThat(fileSystem.delete(renamed, false)).isTrue();
        assertThat(fileSystem.listStatus(dir)).isEmpty();

        // every accepted call is audited with the token that was used for it
        assertThat(audit.lines())
                .allSatisfy(
                        line ->
                                assertThat(line)
                                        .contains("\ttest-process\tACCESS\t")
                                        .contains("token=t-1"))
                .extracting(TokenCheckedFileSystemTest::operationOf)
                .containsExactly(
                        "mkdirs",
                        "create",
                        "listStatus",
                        "open",
                        "getFileStatus",
                        "rename",
                        "getFileStatus",
                        "delete",
                        "listStatus");
        assertThat(audit.lines().get(0)).endsWith("op=mkdirs path=" + dir + " token=t-1");
    }

    private static String operationOf(String auditLine) {
        return auditLine.replaceAll(".*\\top=(\\S+) .*", "$1");
    }

    @Test
    void expiredTokenRejectsEveryOperationAndAuditsAViolation() throws Exception {
        final long expiredSince = System.currentTimeMillis() - 5_000L;
        HeldToken.set(new MockToken("t-old", expiredSince - 30_000L, expiredSince));
        final Path dir = tokenfsPath("ha");

        assertThatThrownBy(() -> fileSystem.listStatus(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("tokenfs rejected listStatus")
                .hasMessageContaining(dir.toString())
                .hasMessageContaining("token t-old is expired since");

        assertThatThrownBy(() -> fileSystem.mkdirs(dir)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> fileSystem.create(dir, FileSystem.WriteMode.OVERWRITE))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> fileSystem.open(dir)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> fileSystem.getFileStatus(dir)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> fileSystem.delete(dir, true)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> fileSystem.rename(dir, dir)).isInstanceOf(IOException.class);

        final List<String> lines = audit.lines();
        assertThat(lines).hasSize(7).allMatch(line -> line.contains("\tVIOLATION\t"));
        assertThat(lines.get(0))
                .contains("\ttest-process\tVIOLATION\t")
                .contains("op=listStatus")
                .contains("token=t-old")
                .contains("reason=expired");
        assertThat(new File(tempDir, "ha")).doesNotExist();
    }

    @Test
    void missingTokenRejectsAndAuditsAViolation() throws Exception {
        final Path dir = tokenfsPath("ha");

        assertThatThrownBy(() -> fileSystem.listStatus(dir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("this process holds no token");

        assertThat(audit.lines()).hasSize(1);
        assertThat(audit.lines().get(0))
                .contains("\tVIOLATION\t")
                .contains("token=none")
                .contains("reason=missing");
    }

    @Test
    void tokenReplacedByARenewalIsHonoredImmediately() throws Exception {
        final long now = System.currentTimeMillis();
        HeldToken.set(new MockToken("t-old", now - 40_000L, now - 10_000L));
        final Path dir = tokenfsPath("ha");
        assertThatThrownBy(() -> fileSystem.mkdirs(dir)).isInstanceOf(IOException.class);

        new MockDelegationTokenReceiver()
                .onNewTokensObtained(new MockToken("t-new", now, now + 60_000L).toBytes());

        assertThat(fileSystem.mkdirs(dir)).isTrue();
        assertThat(new File(tempDir, "ha")).isDirectory();

        // the rejected call was audited with the old token, the accepted one with the new token
        assertThat(audit.lines()).hasSize(3);
        assertThat(audit.lines().get(0)).contains("\tVIOLATION\t").contains("token=t-old");
        assertThat(audit.lines().get(1)).contains("\tUPDATE\t").contains("token=t-new");
        assertThat(audit.lines().get(2))
                .contains("\tACCESS\t")
                .contains("op=mkdirs")
                .contains("token=t-new");
    }

    @Test
    void theFileSystemIsReportedAsSharedStorage() {
        assertThat(fileSystem.isDistributedFS()).isTrue();
    }
}
