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

package com.example.failover.plugin.testing;

import com.example.failover.plugin.audit.Audit;
import com.example.failover.plugin.token.HeldToken;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * Points the audit of the plugin to a temporary directory under a fixed process name for the
 * duration of a test, starts without a held token, and reads back what was audited.
 */
public final class AuditSandbox implements AutoCloseable {

    public static final String PROCESS = "test-process";

    private final File directory;

    private AuditSandbox(File directory) {
        this.directory = directory;
    }

    /** Creates the audit directory below <code>parent</code>. */
    public static AuditSandbox open(File parent) throws IOException {
        final File directory = new File(parent, "audit");
        if (!directory.mkdir()) {
            throw new IOException("Could not create " + directory);
        }
        Audit.configure(directory.toPath(), PROCESS);
        HeldToken.clear();
        return new AuditSandbox(directory);
    }

    public File directory() {
        return directory;
    }

    /** Every line which was audited so far. */
    public List<String> lines() throws IOException {
        final File file = new File(directory, PROCESS + ".audit");
        return file.exists() ? Files.readAllLines(file.toPath()) : List.of();
    }

    @Override
    public void close() {
        HeldToken.clear();
        Audit.reset();
    }
}
