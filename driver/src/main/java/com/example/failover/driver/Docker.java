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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** A thin wrapper around the docker command line. */
final class Docker {

    record Result(int exitCode, String output) {
        boolean ok() {
            return exitCode == 0;
        }
    }

    /** What <code>docker inspect</code> says about a container. */
    record ContainerState(String status, int exitCode, String startedAt, boolean oomKilled) {
        boolean running() {
            return status.equals("running");
        }
    }

    private final String project;
    private final Path composeFile;

    Docker(String project, Path composeFile) {
        this.project = project;
        this.composeFile = composeFile;
    }

    Result run(Duration timeout, String... arguments) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(arguments));
        final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        final byte[][] output = new byte[1][];
        final Thread reader =
                new Thread(
                        () -> {
                            try {
                                output[0] = process.getInputStream().readAllBytes();
                            } catch (IOException e) {
                                output[0] = e.toString().getBytes(StandardCharsets.UTF_8);
                            }
                        },
                        "docker-output-reader");
        reader.setDaemon(true);
        reader.start();
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IOException("Timed out after " + timeout + ": " + String.join(" ", command));
        }
        reader.join(5_000);
        final String text = output[0] == null ? "" : new String(output[0], StandardCharsets.UTF_8);
        return new Result(process.exitValue(), text);
    }

    Result runChecked(Duration timeout, String... arguments)
            throws IOException, InterruptedException {
        final Result result = run(timeout, arguments);
        if (!result.ok()) {
            throw new IOException(
                    "docker "
                            + String.join(" ", arguments)
                            + " failed with exit code "
                            + result.exitCode()
                            + ":\n"
                            + result.output());
        }
        return result;
    }

    void composeUp() throws IOException, InterruptedException {
        runChecked(
                Duration.ofMinutes(10),
                "compose",
                "-p",
                project,
                "-f",
                composeFile.toString(),
                "up",
                "-d");
    }

    void composeDown() throws IOException, InterruptedException {
        run(
                Duration.ofMinutes(3),
                "compose",
                "-p",
                project,
                "-f",
                composeFile.toString(),
                "down",
                "-v",
                "--remove-orphans");
    }

    void kill(String container) throws IOException, InterruptedException {
        runChecked(Duration.ofSeconds(30), "kill", "--signal=KILL", container);
    }

    void start(String container) throws IOException, InterruptedException {
        runChecked(Duration.ofSeconds(60), "start", container);
    }

    void restart(String container) throws IOException, InterruptedException {
        runChecked(Duration.ofSeconds(90), "restart", "-t", "1", container);
    }

    ContainerState state(String container) throws IOException, InterruptedException {
        final Result result =
                runChecked(
                        Duration.ofSeconds(30),
                        "inspect",
                        "-f",
                        "{{.State.Status}}|{{.State.ExitCode}}|{{.State.StartedAt}}|{{.State.OOMKilled}}",
                        container);
        final String[] parts = result.output().trim().split("\\|", -1);
        return new ContainerState(
                parts[0], Integer.parseInt(parts[1]), parts[2], Boolean.parseBoolean(parts[3]));
    }
}
