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

package com.example.failover.plugin;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenCheckedFileSystemTest {

    @TempDir File tempDir;

    private File auditDir;
    private TokenCheckedFileSystem fileSystem;

    @BeforeEach
    void setUp() throws IOException {
        auditDir = new File(tempDir, "audit");
        assertThat(auditDir.mkdir()).isTrue();
        Audit.configure(auditDir.toPath(), "test-process");
        HeldToken.clear();
        fileSystem = new TokenCheckedFileSystem(URI.create("tokenfs:///"));
    }

    @AfterEach
    void tearDown() {
        HeldToken.clear();
        Audit.reset();
    }

    private Path tokenfsPath(String relative) {
        return new Path("tokenfs", null, new File(tempDir, relative).getPath());
    }

    private List<String> auditLines() throws IOException {
        final File file = new File(auditDir, "test-process.audit");
        return file.exists() ? Files.readAllLines(file.toPath()) : List.of();
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
        assertThat(auditLines()).isEmpty();
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

        final List<String> lines = auditLines();
        assertThat(lines).hasSize(7);
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

        assertThat(auditLines()).hasSize(1);
        assertThat(auditLines().get(0))
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
    }

    @Test
    void providerIssuesATokenWhichIsValidForTheConfiguredLifetime() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(MockDelegationTokenProvider.LIFETIME, Duration.ofSeconds(7));
        final MockDelegationTokenProvider provider = new MockDelegationTokenProvider();
        provider.init(configuration);

        final long before = System.currentTimeMillis();
        final var obtained = provider.obtainDelegationTokens();
        final long after = System.currentTimeMillis();

        final MockToken token = MockToken.fromBytes(obtained.getTokens());
        assertThat(token.id()).isEqualTo("test-process-1");
        assertThat(token.issuedAtMillis()).isBetween(before, after);
        assertThat(token.validUntilMillis() - token.issuedAtMillis()).isEqualTo(7_000L);
        assertThat(obtained.getValidUntil()).contains(token.validUntilMillis());
        assertThat(provider.obtainDelegationTokens().getTokens())
                .isNotEqualTo(obtained.getTokens());
        assertThat(auditLines()).hasSize(3); // PROVIDER_INIT and two OBTAIN
        assertThat(auditLines().get(1)).contains("\tOBTAIN\t").contains("token=test-process-1");
    }

    @Test
    void providerFailsWhileTheDriverSwitchedTheServiceOff() throws Exception {
        final MockDelegationTokenProvider provider = new MockDelegationTokenProvider();
        provider.init(new Configuration());
        final File switchedOff = new File(auditDir, MockDelegationTokenProvider.OBTAIN_FAILS_FILE);
        assertThat(switchedOff.createNewFile()).isTrue();

        assertThatThrownBy(provider::obtainDelegationTokens)
                .isInstanceOf(IOException.class)
                .hasMessage("The mock token service is switched off");
        assertThat(auditLines().get(auditLines().size() - 1)).contains("\tOBTAIN_FAILED\t");

        assertThat(switchedOff.delete()).isTrue();
        assertThat(provider.obtainDelegationTokens().getTokens()).isNotEmpty();
    }

    @Test
    void theFileSystemIsReportedAsSharedStorage() {
        assertThat(fileSystem.isDistributedFS()).isTrue();
    }

    @Test
    void receiverInitAuditsItselfAndStartsTheSampler() throws Exception {
        try {
            new MockDelegationTokenReceiver().init(new Configuration());

            assertThat(auditLines()).anyMatch(line -> line.endsWith("\tRECEIVER_INIT\t"));
            assertThat(Sampler.isRunning()).isTrue();
        } finally {
            Sampler.stop();
        }
        assertThat(Sampler.isRunning()).isFalse();
    }

    @Test
    void receiverKeepsTheTokenAndAuditsTheUpdate() throws Exception {
        final MockToken token = new MockToken("jm2-4", 10L, 40L);

        new MockDelegationTokenReceiver().onNewTokensObtained(token.toBytes());

        assertThat(HeldToken.get()).isEqualTo(token);
        assertThat(auditLines()).hasSize(1);
        assertThat(auditLines().get(0)).endsWith("\tUPDATE\ttoken=jm2-4 issuedAt=10 validUntil=40");
    }

    @Test
    void samplerAuditsStaleOnlyWhileTheHeldTokenIsExpired() throws Exception {
        HeldToken.set(new MockToken("t-1", 0L, 100L));

        Sampler.sampleOnce(99L);
        assertThat(auditLines()).isEmpty();

        Sampler.sampleOnce(130L);
        assertThat(auditLines()).hasSize(1);
        assertThat(auditLines().get(0)).endsWith("\tSTALE\ttoken=t-1 expiredForMs=30");

        HeldToken.clear();
        Sampler.sampleOnce(500L);
        assertThat(auditLines()).hasSize(1);
    }
}
