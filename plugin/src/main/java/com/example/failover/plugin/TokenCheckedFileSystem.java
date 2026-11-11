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

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * The local file system behind the scheme <code>tokenfs://</code>, which rejects every call while
 * the token held by this process is missing or expired. This models what S3 does with expired
 * session credentials, and it reads the token exactly like Flink's S3 credential providers read
 * theirs: from a static field of the receiver, on every request.
 *
 * <p>Paths of returned file statuses are mapped back to <code>tokenfs://</code>, otherwise the
 * follow-up calls of Flink would go to the local file system directly and bypass the check. Open
 * streams are not checked again after they were opened.
 */
public class TokenCheckedFileSystem extends FileSystem {

    private final FileSystem local = LocalFileSystem.getSharedInstance();

    private final URI uri;

    public TokenCheckedFileSystem(URI uri) {
        this.uri = uri;
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(TokenCheckedFileSystemFactory.SCHEME, null, "/");
    }

    @Override
    public Path getHomeDirectory() {
        return getWorkingDirectory();
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        checkToken("getFileStatus", f);
        return map(local.getFileStatus(toLocal(f)));
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        checkToken("getFileBlockLocations", file.getPath());
        final FileStatus inner =
                file instanceof MappedFileStatus ? ((MappedFileStatus) file).delegate : file;
        return local.getFileBlockLocations(inner, start, len);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        checkToken("open", f);
        return local.open(toLocal(f), bufferSize);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        checkToken("open", f);
        return local.open(toLocal(f));
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        checkToken("listStatus", f);
        final FileStatus[] statuses = local.listStatus(toLocal(f));
        if (statuses == null) {
            return null;
        }
        final FileStatus[] mapped = new FileStatus[statuses.length];
        for (int i = 0; i < statuses.length; i++) {
            mapped[i] = map(statuses[i]);
        }
        return mapped;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        checkToken("delete", f);
        return local.delete(toLocal(f), recursive);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        checkToken("mkdirs", f);
        return local.mkdirs(toLocal(f));
    }

    @Override
    public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
        checkToken("create", f);
        return local.create(toLocal(f), overwriteMode);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkToken("rename", src);
        return local.rename(toLocal(src), toLocal(dst));
    }

    @Override
    public boolean isDistributedFS() {
        // The containers of the test share the directory, so it is shared storage like S3.
        return true;
    }

    private static void checkToken(String operation, Path path) throws IOException {
        final MockToken held = HeldToken.get();
        final long now = System.currentTimeMillis();
        if (held == null) {
            Audit.log(
                    "VIOLATION",
                    "op=" + operation + " path=" + path + " token=none reason=missing");
            throw new IOException(
                    "tokenfs rejected " + operation + " " + path + ": this process holds no token");
        }
        if (!held.isValidAt(now)) {
            final long expiredForMillis = now - held.validUntilMillis();
            Audit.log(
                    "VIOLATION",
                    "op="
                            + operation
                            + " path="
                            + path
                            + " token="
                            + held.id()
                            + " reason=expired expiredForMs="
                            + expiredForMillis);
            throw new IOException(
                    "tokenfs rejected "
                            + operation
                            + " "
                            + path
                            + ": token "
                            + held.id()
                            + " is expired since "
                            + expiredForMillis
                            + " ms");
        }
    }

    private static Path toLocal(Path path) {
        return new Path("file", null, path.toUri().getPath());
    }

    private static FileStatus map(FileStatus localStatus) {
        return new MappedFileStatus(
                localStatus,
                new Path(
                        TokenCheckedFileSystemFactory.SCHEME,
                        null,
                        localStatus.getPath().toUri().getPath()));
    }

    /** A local file status which reports a <code>tokenfs://</code> path. */
    private static final class MappedFileStatus implements FileStatus {

        private final FileStatus delegate;
        private final Path path;

        private MappedFileStatus(FileStatus delegate, Path path) {
            this.delegate = delegate;
            this.path = path;
        }

        @Override
        public long getLen() {
            return delegate.getLen();
        }

        @Override
        public long getBlockSize() {
            return delegate.getBlockSize();
        }

        @Override
        public short getReplication() {
            return delegate.getReplication();
        }

        @Override
        public long getModificationTime() {
            return delegate.getModificationTime();
        }

        @Override
        public long getAccessTime() {
            return delegate.getAccessTime();
        }

        @Override
        public boolean isDir() {
            return delegate.isDir();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }
}
