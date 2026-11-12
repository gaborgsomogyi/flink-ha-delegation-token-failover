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

package com.example.failover.driver.cluster;

import com.example.failover.driver.evidence.LogLine;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds the leader JobManager from the log lines of its ResourceManager service: <code>
 * Resource manager service is granted leadership with session id ...</code> and <code>
 * Resource manager service is revoked leadership with session id ...</code>
 * (ResourceManagerServiceImpl). The REST endpoint of a standby is not relied on.
 *
 * <p>A process which is killed writes no revoke line, so the caller passes the processes which are
 * alive. A restarted process forgets its earlier grants.
 */
final class LeaderTracker {

    private static final Pattern LINE =
            Pattern.compile(
                    "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) .*Resource manager service is (granted|revoked) leadership");

    private final Map<String, Path> logs;
    private final Map<String, Long> offsets = new HashMap<>();
    private final Map<String, Long> lastGrant = new HashMap<>();
    private final Map<String, Long> lastRevoke = new HashMap<>();
    private final Map<String, Integer> grants = new HashMap<>();

    LeaderTracker(Map<String, Path> logsByProcess) {
        this.logs = logsByProcess;
    }

    /** Reads what was appended to the logs since the last call. */
    void refresh() throws IOException {
        for (Map.Entry<String, Path> entry : logs.entrySet()) {
            final Path log = entry.getValue();
            if (!Files.exists(log)) {
                continue;
            }
            final long offset = offsets.getOrDefault(entry.getKey(), 0L);
            try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "r")) {
                final long length = file.length();
                if (length <= offset) {
                    continue;
                }
                file.seek(offset);
                final byte[] bytes = new byte[(int) (length - offset)];
                file.readFully(bytes);
                // only complete lines, the rest is read next time
                final String text = new String(bytes, StandardCharsets.UTF_8);
                final int lastNewline = text.lastIndexOf('\n');
                if (lastNewline < 0) {
                    continue;
                }
                offsets.put(
                        entry.getKey(),
                        offset
                                + text.substring(0, lastNewline + 1)
                                        .getBytes(StandardCharsets.UTF_8)
                                        .length);
                accept(entry.getKey(), text.substring(0, lastNewline + 1));
            }
        }
    }

    void accept(String process, String text) {
        for (String line : text.split("\n")) {
            final Matcher matcher = LINE.matcher(line);
            if (!matcher.find()) {
                continue;
            }
            final long millis = LogLine.parseTimestamp(matcher.group(1));
            if (matcher.group(2).equals("granted")) {
                lastGrant.put(process, millis);
                grants.merge(process, 1, Integer::sum);
            } else {
                lastRevoke.put(process, millis);
            }
        }
    }

    /** How often the process was granted leadership so far, over all its incarnations. */
    int grantCount(String process) {
        return grants.getOrDefault(process, 0);
    }

    /** Call when the process was started again: its earlier leadership is over. */
    void processRestarted(String process) {
        lastGrant.remove(process);
        lastRevoke.remove(process);
    }

    /** The alive process which was granted leadership last and has not been revoked since. */
    Optional<String> leader(Set<String> aliveProcesses) {
        String leader = null;
        long leaderGrant = Long.MIN_VALUE;
        for (String process : new HashSet<>(aliveProcesses)) {
            final Long grant = lastGrant.get(process);
            if (grant == null) {
                continue;
            }
            final Long revoke = lastRevoke.get(process);
            if (revoke != null && revoke >= grant) {
                continue;
            }
            if (grant > leaderGrant) {
                leaderGrant = grant;
                leader = process;
            }
        }
        return Optional.ofNullable(leader);
    }
}
