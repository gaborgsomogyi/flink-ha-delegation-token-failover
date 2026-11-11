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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderTrackerTest {

    private static final String GRANTED_AT_10 =
            "2026-11-11 20:00:10,000 INFO  org.apache.flink.runtime.resourcemanager"
                    + ".ResourceManagerServiceImpl [] - Resource manager service is granted"
                    + " leadership with session id 6659c190.\n";
    private static final String GRANTED_AT_20 =
            "2026-11-11 20:00:20,000 INFO  org.apache.flink.runtime.resourcemanager"
                    + ".ResourceManagerServiceImpl [] - Resource manager service is granted"
                    + " leadership with session id 7a1.\n";
    private static final String REVOKED_AT_30 =
            "2026-11-11 20:00:30,000 INFO  org.apache.flink.runtime.resourcemanager"
                    + ".ResourceManagerServiceImpl [] - Resource manager service is revoked"
                    + " leadership with session id 7a1.\n";
    private static final String UNRELATED =
            "2026-11-11 20:00:40,000 INFO  org.apache.flink.runtime.dispatcher"
                    + ".DispatcherRestEndpoint [] - http://jm1:8081 was granted leadership with"
                    + " leaderSessionID=6659c190\n";

    @TempDir Path tempDir;

    private final LeaderTracker tracker = new LeaderTracker(Map.of());

    @Test
    void theLeaderIsTheAliveProcessWhichWasGrantedLeadershipLast() {
        tracker.accept("jm1", GRANTED_AT_10);
        tracker.accept("jm3", GRANTED_AT_20);

        assertThat(tracker.leader(Set.of("jm1", "jm2", "jm3"))).contains("jm3");
        assertThat(tracker.leader(Set.of("jm1", "jm2"))).contains("jm1");
    }

    @Test
    void aRevokedProcessIsNoLeader() {
        tracker.accept("jm3", GRANTED_AT_20 + REVOKED_AT_30);

        assertThat(tracker.leader(Set.of("jm3"))).isEmpty();
    }

    @Test
    void aProcessWhichWasGrantedAgainAfterTheRevocationIsTheLeader() {
        tracker.accept("jm3", GRANTED_AT_10 + REVOKED_AT_30.replace("20:00:30", "20:00:15"));
        tracker.accept("jm3", GRANTED_AT_20);

        assertThat(tracker.leader(Set.of("jm3"))).contains("jm3");
    }

    @Test
    void noGrantMeansNoLeader() {
        tracker.accept("jm1", UNRELATED);

        assertThat(tracker.leader(Set.of("jm1"))).isEmpty();
        assertThat(tracker.grantCount("jm1")).isZero();
    }

    @Test
    void grantsAreCountedPerProcessAndSurviveARestart() {
        tracker.accept("jm1", GRANTED_AT_10 + GRANTED_AT_20);
        tracker.accept("jm2", GRANTED_AT_10);

        tracker.processRestarted("jm1");

        assertThat(tracker.grantCount("jm1")).isEqualTo(2);
        assertThat(tracker.grantCount("jm2")).isEqualTo(1);
        assertThat(tracker.grantCount("jm3")).isZero();
        assertThat(tracker.leader(Set.of("jm1"))).isEmpty();
        assertThat(tracker.leader(Set.of("jm2"))).contains("jm2");
    }

    @Test
    void refreshReadsOnlyWhatWasAppendedAndOnlyCompleteLines() throws IOException {
        final Path log = tempDir.resolve("jm1.log");
        final LeaderTracker fromFile = new LeaderTracker(Map.of("jm1", log));

        fromFile.refresh();
        assertThat(fromFile.grantCount("jm1")).as("no file yet").isZero();

        final String firstHalf = GRANTED_AT_10 + GRANTED_AT_20.substring(0, 60);
        Files.writeString(log, firstHalf);
        fromFile.refresh();
        assertThat(fromFile.grantCount("jm1")).as("the incomplete line is not read").isEqualTo(1);

        Files.writeString(
                log,
                GRANTED_AT_20.substring(60),
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
        fromFile.refresh();
        assertThat(fromFile.grantCount("jm1")).as("the line is complete now").isEqualTo(2);

        fromFile.refresh();
        assertThat(fromFile.grantCount("jm1")).as("nothing was appended").isEqualTo(2);
        assertThat(fromFile.leader(Set.of("jm1"))).isEqualTo(Optional.of("jm1"));
    }
}
