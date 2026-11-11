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
}
