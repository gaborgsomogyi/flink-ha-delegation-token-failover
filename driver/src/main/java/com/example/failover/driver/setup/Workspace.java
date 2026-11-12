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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** The directory of one run: configuration, plugin, logs, audit files and shared data. */
public final class Workspace {

    private final Path runDirectory;

    private Workspace(Path runDirectory) {
        this.runDirectory = runDirectory;
    }

    public static Workspace create(RunConfig config) throws IOException {
        final String name =
                "run-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        final Path directory = config.workDir().resolve(name);
        Files.createDirectories(directory);
        return new Workspace(directory);
    }

    public Path runDirectory() {
        return runDirectory;
    }

    public Path auditDirectory() {
        return runDirectory.resolve("audit");
    }

    Path logsDirectory() {
        return runDirectory.resolve("logs");
    }

    public Path logFile(ProcessSpec process) {
        return logsDirectory().resolve(process.name() + ".log");
    }

    public Path composeFile() {
        return runDirectory.resolve("docker-compose.yml");
    }

    /**
     * Creates everything the containers mount: one configuration directory per Flink process (a
     * copy of the distribution's conf/ with the generated config.yaml and log4j configuration), the
     * plugin directory, and the shared directories.
     */
    public void prepare(
            RunConfig config,
            ProcessSpec zookeeper,
            List<ProcessSpec> jobManagers,
            List<ProcessSpec> taskManagers)
            throws IOException {
        for (String directory : List.of("audit", "logs", "data", "plugins/dtm-mock")) {
            Files.createDirectories(runDirectory.resolve(directory));
        }
        if (!Files.isRegularFile(config.pluginJar())) {
            throw new IOException(
                    "The plugin jar "
                            + config.pluginJar()
                            + " does not exist, build it with 'mvn package' or pass --plugin-jar");
        }
        Files.copy(
                config.pluginJar(),
                runDirectory.resolve("plugins/dtm-mock").resolve("dtm-mock-plugin.jar"),
                StandardCopyOption.REPLACE_EXISTING);

        final Map<String, String> distributionEnvOptions =
                DistConfig.envOptions(config.flinkHome().resolve("conf").resolve("config.yaml"));
        for (ProcessSpec process : concat(jobManagers, taskManagers)) {
            final Path confDirectory = runDirectory.resolve("conf").resolve(process.name());
            copyDirectory(config.flinkHome().resolve("conf"), confDirectory);
            Files.writeString(
                    confDirectory.resolve("config.yaml"),
                    Topology.flinkConfig(config, process, distributionEnvOptions));
            Files.writeString(
                    confDirectory.resolve("log4j-console.properties"),
                    Topology.log4jConsoleProperties());
        }
        Files.writeString(
                composeFile(),
                ComposeFile.render(config, runDirectory, zookeeper, jobManagers, taskManagers));
    }

    private static List<ProcessSpec> concat(List<ProcessSpec> first, List<ProcessSpec> second) {
        final List<ProcessSpec> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }

    private static void copyDirectory(Path source, Path target) throws IOException {
        try (Stream<Path> paths = Files.walk(source)) {
            paths.forEach(
                    path -> {
                        try {
                            final Path destination =
                                    target.resolve(source.relativize(path).toString());
                            if (Files.isDirectory(path)) {
                                Files.createDirectories(destination);
                            } else {
                                Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
