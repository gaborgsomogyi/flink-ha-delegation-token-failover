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

package com.example.failover.driver.verify;

import com.example.failover.driver.evidence.AuditEvent;
import com.example.failover.driver.evidence.LogLine;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

class ChecksTest {

    private static AuditEvent event(long millis, String process, String name, String... keyValues) {
        final Map<String, String> details = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            details.put(keyValues[i], keyValues[i + 1]);
        }
        return new AuditEvent(millis, process, name, details);
    }

    // ------------------------------------------------------------------------------------------
    // fatal errors
    // ------------------------------------------------------------------------------------------

    /** What a JobManager logged when the token file system rejected its first access. */
    private static final List<String> JM3_LOG =
            List.of(
                    "2026-11-11 20:49:34,116 INFO  org.apache.flink.runtime.jobmanager"
                            + ".DefaultExecutionPlanStore [] - Stopping DefaultExecutionPlanStore.",
                    "2026-11-11 20:49:34,118 ERROR org.apache.flink.runtime.entrypoint.ClusterEntrypoint"
                            + "        [] - Fatal error occurred in the cluster entrypoint.",
                    "java.util.concurrent.CompletionException: org.apache.flink.util"
                            + ".FlinkRuntimeException: Could not retrieve JobResults of"
                            + " globally-terminated jobs from JobResultStore",
                    "\tat java.base/java.util.concurrent.CompletableFuture.encodeThrowable(Unknown Source)",
                    "\tat java.base/java.lang.Thread.run(Unknown Source)",
                    "Caused by: org.apache.flink.util.FlinkRuntimeException: Could not retrieve"
                            + " JobResults of globally-terminated jobs from JobResultStore",
                    "\tat org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess"
                            + ".getDirtyJobResults(SessionDispatcherLeaderProcess.java:288)",
                    "\t... 4 more",
                    "Caused by: java.io.IOException: tokenfs rejected mkdirs"
                            + " tokenfs:/data/ha/job-result-store/failover: token jm3-1 is expired"
                            + " since 41259 ms",
                    "\tat com.example.failover.plugin.TokenCheckedFileSystem.checkToken"
                            + "(TokenCheckedFileSystem.java:164)",
                    "\t... 6 more",
                    "2026-11-11 20:49:34,120 INFO  org.apache.flink.runtime.resourcemanager"
                            + ".StandaloneResourceManager [] - Updating delegation tokens for 0"
                            + " task manager(s).");

    private static final String JM3_FATAL_MESSAGE =
            "Fatal error occurred in the cluster entrypoint."
                    + " | java.util.concurrent.CompletionException:"
                    + " org.apache.flink.util.FlinkRuntimeException: Could not retrieve JobResults"
                    + " of globally-terminated jobs from JobResultStore"
                    + " | Caused by: org.apache.flink.util.FlinkRuntimeException: Could not retrieve"
                    + " JobResults of globally-terminated jobs from JobResultStore"
                    + " | Caused by: java.io.IOException: tokenfs rejected mkdirs"
                    + " tokenfs:/data/ha/job-result-store/failover: token jm3-1 is expired since"
                    + " 41259 ms";

    @Test
    void aFatalErrorOfTheClusterEntrypointIsReportedWithItsRootCause() {
        assertThat(Checks.fatalErrors(Map.of("jm3", JM3_LOG)))
                .containsExactly(new Violation("FATAL", "jm3", 1794430174118L, JM3_FATAL_MESSAGE));
    }

    @Test
    void aLogWithoutFatalErrorsGivesNoViolation() {
        assertThat(Checks.fatalErrors(Map.of("jm1", JM3_LOG.subList(0, 1)))).isEmpty();
    }

    @Test
    void aFatalErrorOfAThreadPoolIsReported() {
        final List<String> log =
                List.of(
                        "2026-11-11 20:50:01,000 ERROR org.apache.flink.util.FatalExitExceptionHandler"
                                + "               [] - FATAL: Thread 'cluster-io-thread-1' produced"
                                + " an uncaught exception. Stopping the process...",
                        "java.lang.OutOfMemoryError: Java heap space",
                        "\tat com.example.Foo.bar(Foo.java:1)");

        assertThat(Checks.fatalErrors(Map.of("tm1", log)))
                .containsExactly(
                        new Violation(
                                "FATAL",
                                "tm1",
                                LogLine.parseTimestamp("2026-11-11 20:50:01,000"),
                                "FATAL: Thread 'cluster-io-thread-1' produced an uncaught"
                                        + " exception. Stopping the process..."
                                        + " | java.lang.OutOfMemoryError: Java heap space"));
    }

    @Test
    void aFatalErrorAtTheEndOfTheLogHasNoCauses() {
        final List<String> log =
                List.of(
                        "2026-11-11 20:50:01,000 ERROR org.apache.flink.runtime.taskexecutor"
                                + ".TaskManagerRunner [] - Fatal error occurred while executing the"
                                + " TaskManager. Shutting it down...");

        assertThat(Checks.fatalErrors(Map.of("tm2", log)))
                .extracting(Violation::message)
                .containsExactly(
                        "Fatal error occurred while executing the TaskManager. Shutting it down...");
    }

    @Test
    void onlyErrorsAreFatal() {
        final List<String> log =
                List.of(
                        "2026-11-11 20:50:01,000 INFO  a.B [] - Fatal error occurred in the cluster"
                                + " entrypoint.",
                        "2026-11-11 20:50:02,000 ERROR a.B [] - The task failed, a fatal error occurred"
                                + " in the user code.");

        assertThat(Checks.fatalErrors(Map.of("jm1", log))).isEmpty();
    }

    @Test
    void onlyTheFirstFatalErrorOfAProcessIsReportedAndEveryProcessIsChecked() {
        final List<String> twoErrors =
                List.of(
                        "2026-11-11 20:50:01,000 ERROR a.B [] - Fatal error occurred in the first place.",
                        "java.lang.IllegalStateException: first",
                        "2026-11-11 20:50:02,000 ERROR a.B [] - Fatal error occurred a second time.",
                        "java.lang.IllegalStateException: second");
        final Map<String, List<String>> logs = new LinkedHashMap<>();
        logs.put("jm1", twoErrors);
        logs.put("jm2", twoErrors.subList(0, 1));
        logs.put("jm3", List.of());

        assertThat(Checks.fatalErrors(logs))
                .extracting(Violation::process, Violation::message)
                .containsExactly(
                        tuple(
                                "jm1",
                                "Fatal error occurred in the first place."
                                        + " | java.lang.IllegalStateException: first"),
                        tuple("jm2", "Fatal error occurred in the first place."));
    }

    // ------------------------------------------------------------------------------------------
    // token problems
    // ------------------------------------------------------------------------------------------

    @Test
    void staleSamplesOfOneProcessAreCollapsedIntoOneViolation() {
        final List<AuditEvent> events =
                List.of(
                        event(1000, "jm1", "STALE", "token", "jm1-1", "expiredForMs", "14"),
                        event(2000, "jm1", "STALE", "token", "jm1-1", "expiredForMs", "1014"),
                        event(3000, "jm1", "STALE", "token", "jm1-1", "expiredForMs", "2014"));

        assertThat(Checks.tokenProblems(events))
                .containsExactly(
                        new Violation(
                                "STALE",
                                "jm1",
                                1000,
                                "the process held an expired token in 3 sample(s), first:"
                                        + " {token=jm1-1, expiredForMs=14}, last:"
                                        + " {token=jm1-1, expiredForMs=2014}"));
    }

    @Test
    void aSingleRejectedCallIsReportedWithoutALastOccurrence() {
        final List<AuditEvent> events =
                List.of(
                        event(
                                5000,
                                "jm3",
                                "VIOLATION",
                                "op",
                                "mkdirs",
                                "path",
                                "tokenfs:/data/ha/x",
                                "token",
                                "jm3-1",
                                "reason",
                                "expired"));

        assertThat(Checks.tokenProblems(events))
                .containsExactly(
                        new Violation(
                                "VIOLATION",
                                "jm3",
                                5000,
                                "the token file system rejected 1 call(s), first: {op=mkdirs,"
                                        + " path=tokenfs:/data/ha/x, token=jm3-1, reason=expired}"));
    }

    @Test
    void eachProcessAndEachKindIsReportedSeparately() {
        final List<AuditEvent> events =
                List.of(
                        event(1000, "jm1", "STALE", "token", "jm1-1"),
                        event(1500, "jm2", "STALE", "token", "jm2-1"),
                        event(2000, "jm1", "VIOLATION", "op", "mkdirs"));

        assertThat(Checks.tokenProblems(events))
                .extracting(Violation::kind, Violation::process)
                .containsExactly(
                        tuple("STALE", "jm1"), tuple("STALE", "jm2"), tuple("VIOLATION", "jm1"));
    }

    @Test
    void obtainingAndUpdatingTokensIsNoProblem() {
        final List<AuditEvent> events =
                List.of(
                        event(1000, "jm1", "OBTAIN", "token", "jm1-1"),
                        event(1001, "jm1", "UPDATE", "token", "jm1-1"),
                        event(2000, "jm1", "THREADS", "count", "41"));

        assertThat(Checks.tokenProblems(events)).isEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // update cadence
    // ------------------------------------------------------------------------------------------

    private static final Duration PERIOD = Duration.ofSeconds(15);

    private static List<AuditEvent> updates(
            String process, long fromMillis, int count, long everyMillis) {
        final List<AuditEvent> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(
                    event(
                            fromMillis + i * everyMillis,
                            process,
                            "UPDATE",
                            "token",
                            process + "-" + i));
        }
        return events;
    }

    @Test
    void aProcessWhichRenewsAboutEveryPeriodIsFine() {
        final List<AuditEvent> events = updates("jm1", 0, 7, 15_000);
        final List<Incarnation> incarnations =
                List.of(new Incarnation("jm1", 0, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(events, incarnations, PERIOD, 100_000)).isEmpty();
    }

    @Test
    void aProcessWhichRenewsTooRarelyIsReported() {
        final List<AuditEvent> events = updates("jm1", 0, 2, 15_000);
        final List<Incarnation> incarnations =
                List.of(new Incarnation("jm1", 0, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(events, incarnations, PERIOD, 100_000))
                .containsExactly(
                        new Violation(
                                "UPDATE_CADENCE",
                                "jm1",
                                0,
                                "received 2 token update(s) in 100s although 3 to 10 are expected"
                                        + " for a renewal period of 15s"));
    }

    @Test
    void aProcessWhichRenewsTooOftenIsReported() {
        final List<AuditEvent> events = updates("jm1", 0, 11, 5_000);
        final List<Incarnation> incarnations =
                List.of(new Incarnation("jm1", 0, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(events, incarnations, PERIOD, 100_000))
                .extracting(Violation::kind, Violation::message)
                .containsExactly(
                        tuple(
                                "UPDATE_CADENCE",
                                "received 11 token update(s) in 100s although 3 to 10 are expected"
                                        + " for a renewal period of 15s"));
    }

    @Test
    void aTaskManagerMayGetOneExtraUpdateForEveryChangeOfTheLeader() {
        // the leader changes four times, a new leader hands over its newest token when the
        // TaskManager registers, which is one more update than the renewals alone
        final List<AuditEvent> events = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            final String leader = (i / 5) % 2 == 0 ? "jm1" : "jm2";
            events.add(event(i * 12_000L, "tm1", "UPDATE", "token", leader + "-" + i));
        }
        final List<Incarnation> incarnations =
                List.of(new Incarnation("tm1", 0, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(events, incarnations, PERIOD, 300_000)).isEmpty();
    }

    @Test
    void aTaskManagerWhichGetsTooManyUpdatesFromOneLeaderIsReported() {
        final List<AuditEvent> events = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            events.add(event(i * 7_500L, "tm1", "UPDATE", "token", "jm1-" + i));
        }
        final List<Incarnation> incarnations =
                List.of(new Incarnation("tm1", 0, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(events, incarnations, PERIOD, 300_000))
                .extracting(Violation::kind, Violation::message)
                .containsExactly(
                        tuple(
                                "UPDATE_CADENCE",
                                "received 40 token update(s) in 300s although 10 to 23 are expected"
                                        + " for a renewal period of 15s"));
    }

    @Test
    void aShortIncarnationIsNotChecked() {
        final List<Incarnation> incarnations =
                List.of(new Incarnation("jm1", 0, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(List.of(), incarnations, PERIOD, 59_000)).isEmpty();
    }

    @Test
    void onlyTheUpdatesOfTheIncarnationCount() {
        final List<AuditEvent> events = new ArrayList<>();
        events.addAll(updates("jm1", 0, 5, 15_000));
        events.addAll(updates("jm1", 200_000, 5, 15_000));
        events.addAll(updates("jm2", 0, 40, 1_000));
        final List<Incarnation> incarnations =
                List.of(
                        new Incarnation("jm1", 0, 80_000, true),
                        new Incarnation("jm1", 200_000, Incarnation.OPEN, false));

        assertThat(Checks.updateCadence(events, incarnations, PERIOD, 280_000)).isEmpty();
    }

    @Test
    void updatesPerProcessCountsOnlyUpdates() {
        final List<AuditEvent> events = new ArrayList<>();
        events.addAll(updates("jm1", 0, 3, 1_000));
        events.addAll(updates("tm1", 0, 2, 1_000));
        events.add(event(9_000, "jm1", "OBTAIN", "token", "jm1-9"));

        assertThat(Checks.updatesPerProcess(events))
                .containsExactly(entry("jm1", 3L), entry("tm1", 2L));
    }

    // ------------------------------------------------------------------------------------------
    // task managers after a failover
    // ------------------------------------------------------------------------------------------

    private static AuditEvent access(long millis, String process) {
        return event(
                millis,
                process,
                "ACCESS",
                "op",
                "create",
                "path",
                "tokenfs:/data/checkpoints/chk-1/f",
                "token",
                process + "-1");
    }

    @Test
    void accessesPerProcessCountsOnlyAcceptedCalls() {
        final List<AuditEvent> events =
                List.of(
                        access(1_000, "tm1"),
                        access(2_000, "tm1"),
                        access(3_000, "tm2"),
                        event(4_000, "tm2", "VIOLATION", "op", "create"),
                        event(5_000, "tm2", "UPDATE", "token", "tm2-2"));

        assertThat(Checks.accessesPerProcess(events))
                .containsExactly(entry("tm1", 2L), entry("tm2", 1L));
    }

    @Test
    void aTaskManagerWhichGotATokenAndUsedTheFileSystemInTheWindowIsFine() {
        final List<AuditEvent> events =
                List.of(
                        event(15_000, "tm1", "UPDATE", "token", "tm1-2"),
                        event(16_000, "tm2", "UPDATE", "token", "tm2-2"),
                        access(17_000, "tm1"),
                        access(18_000, "tm2"));
        final List<String> taskManagers = List.of("tm1", "tm2");

        assertThat(Checks.tokensDelivered(events, taskManagers, 10_000, 20_000)).isEmpty();
        assertThat(Checks.fileSystemUsed(events, taskManagers, 10_000, 20_000)).isEmpty();
    }

    @Test
    void aTaskManagerWithoutATokenUpdateInTheWindowIsReported() {
        final List<AuditEvent> events =
                List.of(
                        event(15_000, "tm1", "UPDATE", "token", "tm1-2"),
                        // too early, before the failover started
                        event(5_000, "tm2", "UPDATE", "token", "tm2-1"),
                        // too late, after the settle time
                        event(25_000, "tm2", "UPDATE", "token", "tm2-3"));

        assertThat(Checks.tokensDelivered(events, List.of("tm1", "tm2"), 10_000, 20_000))
                .containsExactly(
                        new Violation(
                                "NO_UPDATE",
                                "tm2",
                                10_000,
                                "received no token update in the 10s from the start of the"
                                        + " failover until the end of the settle time"));
    }

    @Test
    void aTaskManagerWhichDidNotUseTheFileSystemInTheWindowIsReported() {
        final List<AuditEvent> events =
                List.of(
                        access(15_000, "tm1"),
                        // a rejected call is no use of the file system
                        event(16_000, "tm2", "VIOLATION", "op", "create", "reason", "expired"),
                        // the other kinds of events do not count either
                        event(17_000, "tm2", "UPDATE", "token", "tm2-2"),
                        // somebody else used it
                        access(18_000, "jm1"));

        assertThat(Checks.fileSystemUsed(events, List.of("tm1", "tm2"), 10_000, 20_000))
                .containsExactly(
                        new Violation(
                                "NO_ACCESS",
                                "tm2",
                                10_000,
                                "did not use the file system in the 10s from the start of the"
                                        + " failover until the end of the settle time"));
    }

    @Test
    void theWindowIncludesItsBoundaries() {
        final List<AuditEvent> events = List.of(access(10_000, "tm1"), access(20_000, "tm2"));

        assertThat(Checks.fileSystemUsed(events, List.of("tm1", "tm2"), 10_000, 20_000)).isEmpty();
        assertThat(Checks.fileSystemUsed(events, List.of("tm1", "tm2"), 10_001, 19_999))
                .extracting(Violation::process)
                .containsExactly("tm1", "tm2");
    }

    @Test
    void noProcessesMeansNothingToReport() {
        assertThat(Checks.fileSystemUsed(List.of(), List.of(), 0, 10_000)).isEmpty();
        assertThat(Checks.tokensDelivered(List.of(), List.of(), 0, 10_000)).isEmpty();
    }
}
