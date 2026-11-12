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

package com.example.failover.plugin.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Appends one line per event to <code>&lt;audit dir&gt;/&lt;process name&gt;.audit</code>. The
 * driver of the failover test reads these files, they are the facts it asserts on.
 *
 * <p>Line format: <code>epochMillis TAB process TAB EVENT TAB key=value key=value ...</code>.
 * Events: PROVIDER_INIT, RECEIVER_INIT, OBTAIN (provider produced a token), UPDATE (receiver was
 * handed a token), STALE (the held token is expired), VIOLATION (the file system rejected a call).
 *
 * <p>The directory and the process name come from the environment variables DTM_AUDIT_DIR and
 * DTM_PROCESS_NAME. Without them the audit is switched off and only the log is written.
 */
public final class Audit {

    static final String ENV_AUDIT_DIR = "DTM_AUDIT_DIR";
    static final String ENV_PROCESS_NAME = "DTM_PROCESS_NAME";

    private static final Logger LOG = LoggerFactory.getLogger(Audit.class);

    private static final Object LOCK = new Object();

    private static Path auditFile;
    private static String processName = "unknown";
    private static boolean configured;

    private Audit() {}

    /** Used by tests, the plugin itself configures from the environment. */
    public static void configure(Path directory, String process) {
        synchronized (LOCK) {
            processName = process;
            auditFile = directory == null ? null : directory.resolve(process + ".audit");
            configured = true;
        }
    }

    public static void reset() {
        synchronized (LOCK) {
            auditFile = null;
            processName = "unknown";
            configured = false;
        }
    }

    public static String processName() {
        synchronized (LOCK) {
            configureFromEnvironmentIfNeeded();
            return processName;
        }
    }

    /** The directory which also holds the control files of the driver, or null. */
    public static Path directory() {
        synchronized (LOCK) {
            configureFromEnvironmentIfNeeded();
            return auditFile == null ? null : auditFile.getParent();
        }
    }

    public static void log(String event, String details) {
        final long now = System.currentTimeMillis();
        LOG.info("{} {}", event, details);
        synchronized (LOCK) {
            configureFromEnvironmentIfNeeded();
            if (auditFile == null) {
                return;
            }
            final String line =
                    now
                            + "\t"
                            + processName
                            + "\t"
                            + event
                            + "\t"
                            + details
                            + System.lineSeparator();
            try {
                Files.write(
                        auditFile,
                        line.getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                LOG.warn("Could not append to the audit file {}", auditFile, e);
            }
        }
    }

    private static void configureFromEnvironmentIfNeeded() {
        if (configured) {
            return;
        }
        configured = true;
        final String directory = System.getenv(ENV_AUDIT_DIR);
        final String process = System.getenv(ENV_PROCESS_NAME);
        if (process != null && !process.isEmpty()) {
            processName = process;
        }
        if (directory != null && !directory.isEmpty()) {
            auditFile = Paths.get(directory).resolve(processName + ".audit");
        }
    }
}
