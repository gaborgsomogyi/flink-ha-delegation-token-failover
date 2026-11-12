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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.io.IOException;
import java.net.URI;

/** Creates the {@link TokenCheckedFileSystem} for the scheme <code>tokenfs</code>. */
public class TokenCheckedFileSystemFactory implements FileSystemFactory {

    public static final String SCHEME = "tokenfs";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        return new TokenCheckedFileSystem(fsUri);
    }
}
