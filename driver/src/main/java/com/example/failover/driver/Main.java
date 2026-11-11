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

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** Entry point: starts the cluster, runs the scenario, prints the report. */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        final RunConfig config;
        try {
            if (args.length == 0 || List.of(args).contains("--help")) {
                System.out.println(RunConfig.USAGE);
                System.exit(args.length == 0 ? 2 : 0);
                return;
            }
            config = RunConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(RunConfig.USAGE);
            System.exit(2);
            return;
        }
        System.exit(run(config));
    }

    /** Returns the exit code: 0 if every fact held, 1 otherwise. */
    static int run(RunConfig config) {
        final Workspace workspace;
        try {
            workspace = Workspace.create(config);
        } catch (IOException e) {
            System.err.println(
                    "Could not create the run directory in " + config.workDir() + ": " + e);
            return 2;
        }
        final Docker docker = Cluster.newDocker(workspace);
        final Cluster cluster = new Cluster(config, workspace, docker);
        if (!config.keep()) {
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        try {
                                            docker.composeDown();
                                        } catch (IOException | InterruptedException e) {
                                            // nothing left to do while the JVM exits
                                        }
                                    }));
        }

        final List<IterationResult> iterations = new ArrayList<>();
        final List<Violation> violations = new ArrayList<>();
        final List<String> evidence = new ArrayList<>();
        boolean scenarioCompleted = false;
        try {
            cluster.up();
            Scenarios.run(config.scenario(), cluster, config, iterations);
            scenarioCompleted = true;
        } catch (ScenarioFailure failure) {
            violations.add(failure.violation());
            if (!failure.evidence().isEmpty()) {
                evidence.add(failure.evidence());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            violations.add(
                    new Violation(
                            "INTERRUPTED",
                            "",
                            System.currentTimeMillis(),
                            "the run was interrupted"));
        } catch (IOException | RuntimeException e) {
            violations.add(new Violation("ERROR", "", System.currentTimeMillis(), e.toString()));
        }

        List<AuditEvent> events = List.of();
        try {
            events = AuditEvent.readAll(workspace.auditDirectory());
        } catch (IOException e) {
            violations.add(
                    new Violation(
                            "ERROR",
                            "",
                            System.currentTimeMillis(),
                            "could not read the audit files: " + e));
        }
        try {
            violations.addAll(Checks.fatalErrors(cluster.logs()));
        } catch (IOException e) {
            violations.add(
                    new Violation(
                            "ERROR",
                            "",
                            System.currentTimeMillis(),
                            "could not read the logs: " + e));
        }
        violations.addAll(Checks.tokenProblems(events));
        if (scenarioCompleted) {
            violations.addAll(
                    Checks.updateCadence(
                            events,
                            cluster.incarnations(),
                            config.renewalPeriod(),
                            System.currentTimeMillis()));
        }

        final String report =
                Report.render(
                        config,
                        workspace,
                        iterations,
                        violations,
                        evidence,
                        Checks.updatesPerProcess(events));
        System.out.println(report);
        try {
            Files.writeString(workspace.runDirectory().resolve("report.txt"), report);
        } catch (IOException e) {
            System.err.println("Could not write report.txt: " + e);
        }

        if (!config.keep()) {
            try {
                cluster.down();
            } catch (IOException | InterruptedException e) {
                System.err.println("Could not remove the containers: " + e);
            }
        }
        return violations.isEmpty() ? 0 : 1;
    }
}
