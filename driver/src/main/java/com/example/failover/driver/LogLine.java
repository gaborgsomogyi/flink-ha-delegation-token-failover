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

package com.example.failover.driver;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The first line of an entry of a Flink log, in the pattern the driver configures: <code>
 * 2026-11-11 20:49:34,118 ERROR org.apache.flink.Logger [] - message</code>. The lines of a stack
 * trace which follow it do not parse.
 */
record LogLine(long millis, String level, String logger, String message) {

    private static final Pattern ENTRY =
            Pattern.compile(
                    "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) (\\w+)\\s+(\\S+)\\s+\\S* - (.*)$");

    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    static Optional<LogLine> parse(String line) {
        final Matcher matcher = ENTRY.matcher(line);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(
                new LogLine(
                        parseTimestamp(matcher.group(1)),
                        matcher.group(2),
                        matcher.group(3),
                        matcher.group(4)));
    }

    /** The containers run in UTC. */
    static long parseTimestamp(String timestamp) {
        return LocalDateTime.parse(timestamp, TIMESTAMP).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
