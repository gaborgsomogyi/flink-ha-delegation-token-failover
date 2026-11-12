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

package com.example.failover.driver.setup;

import com.example.failover.driver.config.RunConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WorkspaceTest {

    private static final String JAVA_OPTS =
            "--add-opens=java.base/java.util=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true";

    @TempDir Path tempDir;

    private RunConfig prepareDistribution(List<String> configYaml) throws IOException {
        final Path distribution = tempDir.resolve("distribution");
        Files.createDirectories(distribution.resolve("bin"));
        Files.createDirectories(distribution.resolve("lib"));
        Files.createDirectories(distribution.resolve("conf"));
        Files.write(distribution.resolve("conf").resolve("config.yaml"), configYaml);
        Files.writeString(distribution.resolve("conf").resolve("masters"), "localhost:8081\n");
        Files.writeString(tempDir.resolve("plugin.jar"), "not a real jar");
        return RunConfig.parse(
                new String[] {
                    "--flink-home",
                    distribution.toString(),
                    "--plugin-jar",
                    tempDir.resolve("plugin.jar").toString(),
                    "--work-dir",
                    tempDir.resolve("runs").toString()
                });
    }

    private static Workspace prepare(RunConfig config) throws IOException {
        final Workspace workspace = Workspace.create(config);
        workspace.prepare(
                config,
                Topology.zookeeper(),
                Topology.jobManagers(config),
                Topology.taskManagers(config));
        return workspace;
    }

    private static List<String> configOf(Workspace workspace, String process) throws IOException {
        return Files.readAllLines(
                workspace.runDirectory().resolve("conf").resolve(process).resolve("config.yaml"));
    }

    @Test
    void everyFlinkProcessGetsTheJvmOptionsOfTheDistribution() throws IOException {
        final RunConfig config =
                prepareDistribution(
                        List.of(
                                "env:",
                                "  java:",
                                "    opts:",
                                "      all: " + JAVA_OPTS,
                                "jobmanager:",
                                "  bind-host: localhost"));

        final Workspace workspace = prepare(config);

        for (String process : List.of("jm1", "jm2", "jm3", "tm1", "tm2")) {
            final List<String> lines = configOf(workspace, process);
            assertThat(lines).as(process).contains("env.java.opts.all: " + JAVA_OPTS);
            assertThat(lines)
                    .as(process + " must not keep the localhost settings of the distribution")
                    .doesNotContain("jobmanager:", "  bind-host: localhost")
                    .contains("jobmanager.bind-host: 0.0.0.0");
        }
    }

    @Test
    void theOtherFilesOfTheConfDirectoryAreCopied() throws IOException {
        final RunConfig config = prepareDistribution(List.of("env.java.opts.all: " + JAVA_OPTS));

        final Workspace workspace = prepare(config);

        assertThat(workspace.runDirectory().resolve("conf/jm2/masters"))
                .hasContent("localhost:8081\n");
    }

    @Test
    void aDistributionWithoutEnvOptionsGivesConfigsWithoutThem() throws IOException {
        final RunConfig config = prepareDistribution(List.of("jobmanager.bind-host: localhost"));

        final Workspace workspace = prepare(config);

        assertThat(configOf(workspace, "jm1").stream().filter(line -> line.startsWith("env.")))
                .isEmpty();
    }
}
