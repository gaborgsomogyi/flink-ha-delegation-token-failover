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

import java.lang.management.ManagementFactory;

/**
 * Checks once per interval whether the token held by this process is expired and audits STALE if
 * so. This makes a process which does not renew its token visible even if nothing accesses the file
 * system, for example a standby JobManager. Every few seconds it also audits the number of threads
 * of the JVM (THREADS), so that the driver can see growth over many failovers without having to
 * enter the container.
 */
final class Sampler {

    static final String ENV_SAMPLE_MILLIS = "DTM_SAMPLE_MS";
    private static final long DEFAULT_SAMPLE_MILLIS = 1000L;
    private static final long THREADS_EVERY_MILLIS = 10_000L;

    private static Thread thread;

    private Sampler() {}

    static synchronized void ensureStarted() {
        if (thread != null) {
            return;
        }
        final long interval = sampleMillis();
        thread =
                new Thread(
                        () -> {
                            long lastThreads = 0;
                            while (!Thread.currentThread().isInterrupted()) {
                                final long now = System.currentTimeMillis();
                                sampleOnce(now);
                                if (now - lastThreads >= THREADS_EVERY_MILLIS) {
                                    threadsOnce();
                                    lastThreads = now;
                                }
                                try {
                                    Thread.sleep(interval);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        },
                        "dtm-token-sampler");
        thread.setDaemon(true);
        thread.start();
    }

    /** Used by tests. */
    static synchronized boolean isRunning() {
        return thread != null && thread.isAlive();
    }

    /** Used by tests, so that the thread does not outlive the test which started it. */
    static synchronized void stop() throws InterruptedException {
        if (thread != null) {
            thread.interrupt();
            thread.join();
            thread = null;
        }
    }

    /** One check, separate from the loop so that tests do not need a clock. */
    static void sampleOnce(long nowMillis) {
        final MockToken held = HeldToken.get();
        if (held != null && !held.isValidAt(nowMillis)) {
            Audit.log(
                    "STALE",
                    "token="
                            + held.id()
                            + " expiredForMs="
                            + (nowMillis - held.validUntilMillis()));
        }
    }

    /** Audits how many threads this JVM has. */
    static void threadsOnce() {
        Audit.log("THREADS", "count=" + ManagementFactory.getThreadMXBean().getThreadCount());
    }

    private static long sampleMillis() {
        final String configured = System.getenv(ENV_SAMPLE_MILLIS);
        if (configured == null || configured.isEmpty()) {
            return DEFAULT_SAMPLE_MILLIS;
        }
        return Long.parseLong(configured);
    }
}
