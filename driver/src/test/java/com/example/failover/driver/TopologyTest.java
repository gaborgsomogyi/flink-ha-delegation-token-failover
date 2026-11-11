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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyTest {

    private static final String JAVA_OPTS =
            "--add-opens=java.base/java.util=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true";

    @TempDir Path tempDir;

    private RunConfig config(String... extraArguments) throws IOException {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("lib"));
        final List<String> arguments =
                new ArrayList<>(
                        List.of(
                                "--flink-home",
                                tempDir.toString(),
                                "--plugin-jar",
                                tempDir.resolve("plugin.jar").toString()));
        arguments.addAll(List.of(extraArguments));
        return RunConfig.parse(arguments.toArray(new String[0]));
    }

    private static ProcessSpec jobManager(String name) {
        return new ProcessSpec(name, ProcessSpec.Role.JOB_MANAGER, 18081);
    }

    @Test
    void theEnvOptionsOfTheDistributionEndUpInTheConfig() throws IOException {
        final String yaml =
                Topology.flinkConfig(
                        config(), jobManager("jm1"), Map.of("env.java.opts.all", JAVA_OPTS));

        assertThat(yaml.lines()).contains("env.java.opts.all: " + JAVA_OPTS);
    }

    @Test
    void theConfigHasNoEnvKeysIfTheDistributionHasNone() throws IOException {
        final String yaml = Topology.flinkConfig(config(), jobManager("jm1"), Map.of());

        assertThat(yaml.lines().filter(line -> line.startsWith("env."))).isEmpty();
    }

    @Test
    void theEnvOptionsCanBeReadBackFromTheGeneratedConfig() throws IOException {
        final Map<String, String> options =
                Map.of("env.java.opts.all", JAVA_OPTS, "env.java.opts.jobmanager", "-Dx=1");

        final String yaml = Topology.flinkConfig(config(), jobManager("jm1"), options);

        assertThat(DistConfig.envOptions(yaml.lines().toList()))
                .containsExactlyInAnyOrderEntriesOf(options);
    }

    @Test
    void theTokenAndStorageSettingsAreWritten() throws IOException {
        final String yaml =
                Topology.flinkConfig(
                        config("--token-lifetime", "45s", "--renewal-ratio", "0.25"),
                        jobManager("jm1"),
                        Map.of());

        assertThat(yaml.lines())
                .contains(
                        "security.delegation.tokens.enabled: true",
                        "security.delegation.tokens.renewal.time-ratio: 0.25",
                        "security.delegation.token.provider.mock-expiring.lifetime: 45000 ms",
                        "high-availability.type: zookeeper",
                        "high-availability.storageDir: tokenfs:///data/ha",
                        "state.checkpoints.dir: tokenfs:///data/checkpoints");
    }

    @Test
    void eachProcessAddressesItselfByItsContainerName() throws IOException {
        final String yaml = Topology.flinkConfig(config(), jobManager("jm2"), Map.of());

        assertThat(yaml.lines())
                .contains(
                        "jobmanager.rpc.address: jm2",
                        "rest.address: jm2",
                        "taskmanager.host: jm2");
    }
}
