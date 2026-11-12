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

package com.example.failover.driver.report;

import com.example.failover.driver.config.RunConfig;
import com.example.failover.driver.scenario.IterationResult;
import com.example.failover.driver.setup.Workspace;
import com.example.failover.driver.verify.Violation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReportTest {

    @TempDir Path tempDir;

    private Path distribution;

    @BeforeEach
    void createDistribution() throws IOException {
        distribution = tempDir.resolve("distribution");
        Files.createDirectories(distribution.resolve("bin"));
        Files.createDirectories(distribution.resolve("lib"));
    }

    private String render(
            List<IterationResult> iterations, List<Violation> violations, String... options)
            throws IOException {
        final List<String> arguments =
                new ArrayList<>(
                        List.of(
                                "--flink-home",
                                distribution.toString(),
                                "--work-dir",
                                tempDir.resolve("runs").toString()));
        arguments.addAll(List.of(options));
        final RunConfig config = RunConfig.parse(arguments.toArray(new String[0]));
        final Map<String, Long> updates = new LinkedHashMap<>();
        updates.put("jm1", 5L);
        updates.put("tm1", 7L);
        final Map<String, Long> accesses = new LinkedHashMap<>();
        accesses.put("tm1", 233L);
        return Report.render(
                config,
                Workspace.create(config),
                iterations,
                violations,
                List.of(),
                updates,
                accesses);
    }

    private static IterationResult failover() {
        return new IterationResult(1, "killed jm1", "jm3", 5, 5, 6, Map.of("jm2", 52, "jm3", 87));
    }

    @Test
    void aRunWithoutViolationsPassesAndShowsWhatEveryProcessDid() throws IOException {
        final String report = render(List.of(failover()), List.of());

        assertThat(report)
                .contains("cluster:        3 JobManagers, 2 TaskManagers")
                .contains("failovers: 1")
                .contains("#1 killed jm1, new leader jm3 after 5s, job RUNNING after 5s")
                .contains("token updates received per process: {jm1=5, tm1=7}")
                .contains("file system calls accepted per process: {tm1=233}")
                .contains("RESULT: PASSED")
                .doesNotContain("fault injected");
    }

    @Test
    void violationsAreListedAndFailTheRun() throws IOException {
        final String report =
                render(
                        List.of(),
                        List.of(
                                new Violation(
                                        "NO_ACCESS",
                                        "tm1",
                                        1_000L,
                                        "did not use the file system")));

        assertThat(report)
                .contains("RESULT: FAILED, 1 violation(s)")
                .contains("[NO_ACCESS] 1970-01-01T00:00:01Z tm1: did not use the file system")
                .doesNotContain("RESULT: PASSED");
    }

    @Test
    void aFaultInjectionIsSaidToExpectAFailure() throws IOException {
        final String report = render(List.of(), List.of(), "--drop-tokens-of", "tm1");

        assertThat(report)
                .contains(
                        "fault injected: tm1 drops every token update from the start of each"
                                + " failover, the run is expected to FAIL");
    }
}
