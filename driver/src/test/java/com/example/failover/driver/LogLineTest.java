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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogLineTest {

    @Test
    void parsesTheFirstLineOfAnErrorEntry() {
        final LogLine line =
                LogLine.parse(
                                "2026-11-11 20:49:34,118 ERROR"
                                        + " org.apache.flink.runtime.entrypoint.ClusterEntrypoint"
                                        + "        [] - Fatal error occurred in the cluster entrypoint.")
                        .orElseThrow();

        assertThat(line.millis()).isEqualTo(1794430174118L);
        assertThat(line.level()).isEqualTo("ERROR");
        assertThat(line.logger())
                .isEqualTo("org.apache.flink.runtime.entrypoint.ClusterEntrypoint");
        assertThat(line.message()).isEqualTo("Fatal error occurred in the cluster entrypoint.");
    }

    @Test
    void parsesTheLevelWhichIsPaddedToFiveCharacters() {
        final LogLine line =
                LogLine.parse(
                                "2026-11-11 20:33:29,030 INFO  org.apache.flink.Provider [] - Hadoop FS"
                                        + " is not available.")
                        .orElseThrow();

        assertThat(line.level()).isEqualTo("INFO");
        assertThat(line.logger()).isEqualTo("org.apache.flink.Provider");
        assertThat(line.message()).isEqualTo("Hadoop FS is not available.");
    }

    @Test
    void theMessageIsEverythingAfterTheFirstSeparator() {
        final LogLine line =
                LogLine.parse("2026-11-11 20:33:29,030 WARN  a.B [] - x - y [] - z").orElseThrow();

        assertThat(line.message()).isEqualTo("x - y [] - z");
    }

    @Test
    void theLinesOfAStackTraceDoNotParse() {
        assertThat(LogLine.parse("\tat java.base/java.lang.Thread.run(Thread.java:840)")).isEmpty();
        assertThat(LogLine.parse("Caused by: java.io.IOException: x")).isEmpty();
        assertThat(LogLine.parse("java.util.concurrent.CompletionException: boom")).isEmpty();
        assertThat(LogLine.parse("")).isEmpty();
    }

    @Test
    void timestampsAreUtc() {
        assertThat(LogLine.parseTimestamp("1970-01-01 00:00:01,500")).isEqualTo(1500L);
    }
}
