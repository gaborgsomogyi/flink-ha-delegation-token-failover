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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The assertions on the audit files and the logs, as pure functions of the recorded facts. */
final class Checks {

    private Checks() {}

    /**
     * A Flink process which logs a fatal error at ERROR level stops: "Fatal error occurred ..."
     * (the cluster entrypoint and the TaskManager) or "FATAL: Thread ... produced an uncaught
     * exception" (the handler of the thread pools). The first one of each process is reported with
     * its causes, the last of them being the root cause.
     *
     * @param logLinesByProcess the lines of the log file of each process
     */
    static List<Violation> fatalErrors(Map<String, List<String>> logLinesByProcess) {
        final List<Violation> violations = new ArrayList<>();
        for (Map.Entry<String, List<String>> log : logLinesByProcess.entrySet()) {
            final List<String> lines = log.getValue();
            for (int i = 0; i < lines.size(); i++) {
                final Optional<LogLine> entry = LogLine.parse(lines.get(i));
                if (entry.isEmpty()
                        || !entry.get().level().equals("ERROR")
                        || !isFatal(entry.get().message())) {
                    continue;
                }
                final StringBuilder text = new StringBuilder(entry.get().message());
                for (int j = i + 1;
                        j < lines.size() && LogLine.parse(lines.get(j)).isEmpty();
                        j++) {
                    text.append('\n').append(lines.get(j));
                }
                violations.add(
                        new Violation(
                                "FATAL",
                                log.getKey(),
                                entry.get().millis(),
                                Causes.summarize(text.toString(), 2)));
                break;
            }
        }
        return violations;
    }

    private static boolean isFatal(String message) {
        return message.startsWith("Fatal error occurred") || message.startsWith("FATAL: Thread");
    }

    /**
     * Any token the file system rejected (VIOLATION) and any moment a process held an expired token
     * (STALE) is a violation. Repeated events of one process are collapsed into one entry with
     * their count, the first occurrence and the last.
     */
    static List<Violation> tokenProblems(List<AuditEvent> events) {
        final Map<String, List<AuditEvent>> byKey = new LinkedHashMap<>();
        for (AuditEvent event : events) {
            if (event.is("VIOLATION") || event.is("STALE")) {
                byKey.computeIfAbsent(event.process() + "/" + event.event(), k -> new ArrayList<>())
                        .add(event);
            }
        }
        final List<Violation> violations = new ArrayList<>();
        for (List<AuditEvent> group : byKey.values()) {
            final AuditEvent first = group.get(0);
            final AuditEvent last = group.get(group.size() - 1);
            final String what =
                    first.is("VIOLATION")
                            ? "the token file system rejected "
                                    + group.size()
                                    + " call(s), first: "
                                    + describe(first)
                            : "the process held an expired token in "
                                    + group.size()
                                    + " sample(s), first: "
                                    + describe(first);
            violations.add(
                    new Violation(
                            first.event(),
                            first.process(),
                            first.millis(),
                            what + (group.size() > 1 ? ", last: " + describe(last) : "")));
        }
        return violations;
    }

    private static String describe(AuditEvent event) {
        return event.details().toString();
    }

    /**
     * Every incarnation which lived long enough must have received tokens at about the renewal
     * period: not much less (a standby or a former leader which does not renew) and not much more
     * (a duplicated renewal chain).
     */
    static List<Violation> updateCadence(
            List<AuditEvent> events,
            List<Incarnation> incarnations,
            Duration period,
            long nowMillis) {
        final List<Violation> violations = new ArrayList<>();
        final long periodMillis = period.toMillis();
        for (Incarnation incarnation : incarnations) {
            final long length = incarnation.lengthMillis(nowMillis);
            if (length < 4 * periodMillis) {
                continue;
            }
            final long end = Math.min(incarnation.endMillis(), nowMillis);
            final long updates =
                    events.stream()
                            .filter(e -> e.is("UPDATE"))
                            .filter(e -> e.process().equals(incarnation.process()))
                            .filter(
                                    e ->
                                            e.millis() >= incarnation.startMillis()
                                                    && e.millis() <= end)
                            .count();
            final long minimum = length / (2 * periodMillis);
            final long maximum = (length + periodMillis - 1) / periodMillis + 3;
            if (updates < minimum || updates > maximum) {
                violations.add(
                        new Violation(
                                "UPDATE_CADENCE",
                                incarnation.process(),
                                incarnation.startMillis(),
                                "received "
                                        + updates
                                        + " token update(s) in "
                                        + Duration.ofMillis(length).toSeconds()
                                        + "s although "
                                        + minimum
                                        + " to "
                                        + maximum
                                        + " are expected for a renewal period of "
                                        + period.toSeconds()
                                        + "s"));
            }
        }
        return violations;
    }

    /** Number of UPDATE events per process, for the report. */
    static Map<String, Long> updatesPerProcess(List<AuditEvent> events) {
        final Map<String, Long> counts = new LinkedHashMap<>();
        for (AuditEvent event : events) {
            if (event.is("UPDATE")) {
                counts.merge(event.process(), 1L, Long::sum);
            }
        }
        return counts;
    }
}
