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

package com.example.failover.driver.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** All settings of a run, parsed from the command line. */
public record RunConfig(
        Path flinkHome,
        Path pluginJar,
        String scenario,
        Duration duration,
        int jobManagers,
        int taskManagers,
        Duration tokenLifetime,
        double renewalRatio,
        Duration standbyAge,
        Duration failoverInterval,
        Duration settle,
        Path workDir,
        boolean keep,
        String runtimeImage,
        String zookeeperImage,
        String jobManagerMemory,
        String taskManagerMemory,
        int restPortBase) {

    public static final String USAGE =
            String.join(
                    System.lineSeparator(),
                    "Usage: failover-driver --flink-home <distribution> [options]",
                    "",
                    "  --flink-home <dir>        a built Flink distribution (the directory with bin/ and lib/)",
                    "  --plugin-jar <file>       the mock token plugin (default: plugin/target/*-plugin.jar)",
                    "  --scenario <name>         aged-standby (default), rolling, zk-blip",
                    "  --duration <d>            how long 'rolling' keeps failing over, e.g. 30m (default 10m)",
                    "  --job-managers <n>        default 3",
                    "  --task-managers <n>       default 2",
                    "  --token-lifetime <d>      lifetime of a mock token, default 30s",
                    "  --renewal-ratio <r>       security.delegation.tokens.renewal.time-ratio, default 0.5",
                    "  --standby-age <d>         standbys must be up this long before the leader is killed,",
                    "                            default 2 x lifetime + 10s",
                    "  --failover-interval <d>   'rolling': time between two failovers, default 90s",
                    "  --settle <d>              how long the cluster is watched after a failover (no process",
                    "                            may exit, the job must keep checkpointing), default 15s",
                    "  --work-dir <dir>          where runs are created, default ./runs",
                    "  --keep                    leave the containers and the run directory in place",
                    "  --runtime-image <image>   default eclipse-temurin:17-jre",
                    "  --zookeeper-image <image> default zookeeper:3.9",
                    "  --jm-memory <size>        default 1024m",
                    "  --tm-memory <size>        default 1280m",
                    "  --rest-port-base <port>   host port of the REST endpoint of jm1, default 18081");

    public static RunConfig parse(String[] args) {
        final Map<String, String> options = new HashMap<>();
        boolean keep = false;
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            if (arg.equals("--keep")) {
                keep = true;
            } else if (arg.startsWith("--") && i + 1 < args.length) {
                options.put(arg, args[++i]);
            } else {
                throw new IllegalArgumentException("Unknown or incomplete option: " + arg);
            }
        }

        final String home = options.get("--flink-home");
        if (home == null) {
            throw new IllegalArgumentException("--flink-home is required");
        }
        final Duration tokenLifetime =
                parseDuration(options.getOrDefault("--token-lifetime", "30s"));
        final Duration standbyAge =
                options.containsKey("--standby-age")
                        ? parseDuration(options.get("--standby-age"))
                        : tokenLifetime.multipliedBy(2).plusSeconds(10);

        final RunConfig config =
                new RunConfig(
                        Paths.get(home).toAbsolutePath().normalize(),
                        findPluginJar(options.get("--plugin-jar")),
                        options.getOrDefault("--scenario", "aged-standby"),
                        parseDuration(options.getOrDefault("--duration", "10m")),
                        Integer.parseInt(options.getOrDefault("--job-managers", "3")),
                        Integer.parseInt(options.getOrDefault("--task-managers", "2")),
                        tokenLifetime,
                        Double.parseDouble(options.getOrDefault("--renewal-ratio", "0.5")),
                        standbyAge,
                        parseDuration(options.getOrDefault("--failover-interval", "90s")),
                        parseDuration(options.getOrDefault("--settle", "15s")),
                        Paths.get(options.getOrDefault("--work-dir", "runs"))
                                .toAbsolutePath()
                                .normalize(),
                        keep,
                        options.getOrDefault("--runtime-image", "eclipse-temurin:17-jre"),
                        options.getOrDefault("--zookeeper-image", "zookeeper:3.9"),
                        options.getOrDefault("--jm-memory", "1024m"),
                        options.getOrDefault("--tm-memory", "1280m"),
                        Integer.parseInt(options.getOrDefault("--rest-port-base", "18081")));
        config.validate();
        return config;
    }

    void validate() {
        if (!Files.isDirectory(flinkHome.resolve("bin"))
                || !Files.isDirectory(flinkHome.resolve("lib"))) {
            throw new IllegalArgumentException(
                    flinkHome + " is not a Flink distribution (no bin/ or lib/)");
        }
        if (jobManagers < 2) {
            throw new IllegalArgumentException(
                    "At least two JobManagers are needed for a failover");
        }
        if (taskManagers < 1) {
            throw new IllegalArgumentException("At least one TaskManager is needed");
        }
        if (renewalRatio <= 0 || renewalRatio >= 1) {
            throw new IllegalArgumentException("The renewal ratio must be in (0, 1)");
        }
        if (!List.of("aged-standby", "rolling", "zk-blip").contains(scenario)) {
            throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    /** The time between two renewals of a healthy process. */
    public Duration renewalPeriod() {
        return Duration.ofMillis(Math.round(tokenLifetime.toMillis() * renewalRatio));
    }

    /** 30s, 5m, 2h or an ISO-8601 duration. */
    static Duration parseDuration(String text) {
        final String trimmed = text.trim();
        if (trimmed.toUpperCase().startsWith("P")) {
            return Duration.parse(trimmed);
        }
        final long value = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
        switch (trimmed.charAt(trimmed.length() - 1)) {
            case 's':
                return Duration.ofSeconds(value);
            case 'm':
                return Duration.ofMinutes(value);
            case 'h':
                return Duration.ofHours(value);
            default:
                throw new IllegalArgumentException("Not a duration (use 30s, 5m or 2h): " + text);
        }
    }

    private static Path findPluginJar(String configured) {
        if (configured != null) {
            return Paths.get(configured).toAbsolutePath().normalize();
        }
        final Path candidate =
                Paths.get("plugin/target/flink-ha-delegation-token-failover-plugin.jar")
                        .toAbsolutePath()
                        .normalize();
        return candidate;
    }
}
