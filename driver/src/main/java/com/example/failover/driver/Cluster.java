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

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The running cluster: ZooKeeper, JobManagers and TaskManagers in containers. All operations a
 * scenario needs, and the bookkeeping of which process lived when (the incarnations).
 */
final class Cluster {

    private final RunConfig config;
    private final Workspace workspace;
    private final Docker docker;
    private final Rest rest = new Rest();
    private final ProcessSpec zookeeper = Topology.zookeeper();
    private final List<ProcessSpec> jobManagers;
    private final List<ProcessSpec> taskManagers;
    private final LeaderTracker leaderTracker;

    private final List<Incarnation> closedIncarnations = new ArrayList<>();
    private final Map<String, Long> openSince = new HashMap<>();
    private final Set<String> down = new HashSet<>();

    private String jobId;

    Cluster(RunConfig config, Workspace workspace, Docker docker) {
        this.config = config;
        this.workspace = workspace;
        this.docker = docker;
        this.jobManagers = Topology.jobManagers(config);
        this.taskManagers = Topology.taskManagers(config);
        final Map<String, Path> logs = new HashMap<>();
        for (ProcessSpec jobManager : jobManagers) {
            logs.put(jobManager.name(), workspace.logFile(jobManager));
        }
        this.leaderTracker = new LeaderTracker(logs);
    }

    static Docker newDocker(Workspace workspace) {
        return new Docker("dtfailover", workspace.composeFile());
    }

    List<ProcessSpec> jobManagers() {
        return jobManagers;
    }

    List<ProcessSpec> taskManagers() {
        return taskManagers;
    }

    private List<ProcessSpec> flinkProcesses() {
        final List<ProcessSpec> all = new ArrayList<>(jobManagers);
        all.addAll(taskManagers);
        return all;
    }

    // ------------------------------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------------------------------

    void up() throws IOException, InterruptedException {
        workspace.prepare(config, zookeeper, jobManagers, taskManagers);
        docker.composeUp();
        final long now = System.currentTimeMillis();
        for (ProcessSpec process : flinkProcesses()) {
            openSince.put(process.name(), now);
        }
    }

    void down() throws IOException, InterruptedException {
        final long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : new HashMap<>(openSince).entrySet()) {
            closedIncarnations.add(new Incarnation(entry.getKey(), entry.getValue(), now, true));
            openSince.remove(entry.getKey());
        }
        docker.composeDown();
    }

    /** Every incarnation so far, the running ones are open. */
    List<Incarnation> incarnations() {
        final List<Incarnation> all = new ArrayList<>(closedIncarnations);
        for (Map.Entry<String, Long> entry : openSince.entrySet()) {
            all.add(new Incarnation(entry.getKey(), entry.getValue(), Incarnation.OPEN, false));
        }
        return all;
    }

    void kill(ProcessSpec process) throws IOException, InterruptedException {
        docker.kill(process.container());
        down.add(process.name());
        closeIncarnation(process.name(), true);
    }

    void startAgain(ProcessSpec process) throws IOException, InterruptedException {
        docker.start(process.container());
        down.remove(process.name());
        openSince.put(process.name(), System.currentTimeMillis());
        leaderTracker.processRestarted(process.name());
    }

    /** Stops ZooKeeper for the given time, long enough for every session to expire. */
    void blinkZookeeper(Duration offline) throws IOException, InterruptedException {
        docker.runChecked(Duration.ofSeconds(60), "stop", "-t", "1", zookeeper.container());
        Thread.sleep(offline.toMillis());
        docker.start(zookeeper.container());
    }

    private void closeIncarnation(String process, boolean killedByDriver) {
        final Long since = openSince.remove(process);
        if (since != null) {
            closedIncarnations.add(
                    new Incarnation(process, since, System.currentTimeMillis(), killedByDriver));
        }
    }

    // ------------------------------------------------------------------------------------------
    // observations
    // ------------------------------------------------------------------------------------------

    private Set<String> aliveJobManagers() {
        final Set<String> alive = new HashSet<>();
        for (ProcessSpec jobManager : jobManagers) {
            if (!down.contains(jobManager.name())) {
                alive.add(jobManager.name());
            }
        }
        return alive;
    }

    Optional<ProcessSpec> currentLeader(Set<String> excluded) throws IOException {
        leaderTracker.refresh();
        final Set<String> candidates = aliveJobManagers();
        candidates.removeAll(excluded);
        return leaderTracker.leader(candidates).map(this::jobManager);
    }

    private ProcessSpec jobManager(String name) {
        return jobManagers.stream().filter(p -> p.name().equals(name)).findFirst().orElseThrow();
    }

    /** How often a JobManager was granted leadership so far, all JobManagers together. */
    int totalLeadershipGrants() throws IOException {
        leaderTracker.refresh();
        int total = 0;
        for (ProcessSpec jobManager : jobManagers) {
            total += leaderTracker.grantCount(jobManager.name());
        }
        return total;
    }

    /** The age of the incarnation which is running now, or 0. */
    long ageMillis(ProcessSpec process) {
        final Long since = openSince.get(process.name());
        return since == null ? 0 : System.currentTimeMillis() - since;
    }

    /** The newest thread count the process audited in its current incarnation, or -1. */
    int threadCount(ProcessSpec process) {
        final Long since = openSince.get(process.name());
        if (since == null) {
            return -1;
        }
        try {
            int count = -1;
            for (AuditEvent event : AuditEvent.readAll(workspace.auditDirectory())) {
                if (event.is("THREADS")
                        && event.process().equals(process.name())
                        && event.millis() >= since) {
                    count = Integer.parseInt(event.detail("count"));
                }
            }
            return count;
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
    }

    private Optional<JsonNode> restOfAnyJobManager(String path) {
        for (ProcessSpec jobManager : jobManagers) {
            if (down.contains(jobManager.name())) {
                continue;
            }
            final Optional<JsonNode> body = rest.get(jobManager.restHostPort(), path);
            if (body.isPresent()) {
                return body;
            }
        }
        return Optional.empty();
    }

    boolean jobIsRunning() {
        final Optional<JsonNode> overview = restOfAnyJobManager("/jobs/overview");
        if (overview.isEmpty()) {
            return false;
        }
        for (JsonNode job : overview.get().path("jobs")) {
            final boolean ours = jobId == null || job.path("jid").asText().equals(jobId);
            if (ours && job.path("state").asText().equals("RUNNING")) {
                return true;
            }
        }
        return false;
    }

    int completedCheckpoints() {
        if (jobId == null) {
            return -1;
        }
        return restOfAnyJobManager("/jobs/" + jobId + "/checkpoints")
                .map(body -> body.path("counts").path("completed").asInt(-1))
                .orElse(-1);
    }

    /** The lines of the log file of every Flink process which has one, by process name. */
    Map<String, List<String>> logs() throws IOException {
        final Map<String, List<String>> logs = new LinkedHashMap<>();
        for (ProcessSpec process : flinkProcesses()) {
            final Path log = workspace.logFile(process);
            if (Files.exists(log)) {
                final String text = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                logs.put(process.name(), List.of(text.split("\\R")));
            }
        }
        return logs;
    }

    String logTail(ProcessSpec process, int lines) {
        final Path log = workspace.logFile(process);
        if (!Files.exists(log)) {
            return "(no log file " + log + ")";
        }
        try {
            final List<String> all = Files.readAllLines(log);
            return String.join(
                    System.lineSeparator(),
                    all.subList(Math.max(0, all.size() - lines), all.size()));
        } catch (IOException e) {
            return "(could not read " + log + ": " + e + ")";
        }
    }

    // ------------------------------------------------------------------------------------------
    // waiting
    // ------------------------------------------------------------------------------------------

    /** Fails if a process which is not intentionally down is not running. */
    void assertNoUnexpectedExit() throws ScenarioFailure, IOException, InterruptedException {
        for (ProcessSpec process : flinkProcesses()) {
            if (down.contains(process.name())) {
                continue;
            }
            final Docker.ContainerState state = docker.state(process.container());
            if (!state.running()) {
                closeIncarnation(process.name(), false);
                down.add(process.name());
                throw new ScenarioFailure(
                        new Violation(
                                "PROCESS_EXIT",
                                process.name(),
                                System.currentTimeMillis(),
                                "exited unexpectedly, status="
                                        + state.status()
                                        + ", exit code "
                                        + state.exitCode()
                                        + " ("
                                        + describeExitCode(state.exitCode())
                                        + ")"
                                        + (state.oomKilled() ? ", killed by the OOM killer" : "")),
                        "last lines of the log of "
                                + process.name()
                                + ":\n"
                                + logTail(process, 40));
            }
        }
    }

    static String describeExitCode(int exitCode) {
        switch (exitCode) {
            case 2:
                return "a fatal error in the cluster entrypoint";
            case 239:
                return "an uncaught exception in a thread of a pool, Flink exits with -17";
            case 137:
                return "killed";
            case 143:
                return "terminated";
            case 0:
                return "a regular shutdown";
            default:
                return "unknown";
        }
    }

    <T> T await(String what, Duration timeout, Supplier<Optional<T>> condition)
            throws ScenarioFailure, IOException, InterruptedException {
        final long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (true) {
            assertNoUnexpectedExit();
            final Optional<T> result = condition.get();
            if (result.isPresent()) {
                return result.get();
            }
            if (System.currentTimeMillis() > deadline) {
                throw new ScenarioFailure(
                        new Violation(
                                "TIMEOUT",
                                "",
                                System.currentTimeMillis(),
                                "gave up after " + timeout.toSeconds() + "s waiting for " + what),
                        "");
            }
            Thread.sleep(1_000);
        }
    }

    ProcessSpec awaitLeader(Set<String> excluded, Duration timeout)
            throws ScenarioFailure, IOException, InterruptedException {
        return await(
                "a leader JobManager",
                timeout,
                () -> {
                    try {
                        return currentLeader(excluded);
                    } catch (IOException e) {
                        return Optional.empty();
                    }
                });
    }

    void awaitJobRunning(Duration timeout)
            throws ScenarioFailure, IOException, InterruptedException {
        await(
                "the job to be RUNNING",
                timeout,
                () -> jobIsRunning() ? Optional.of(true) : Optional.empty());
    }

    void awaitCheckpointsAbove(int count, Duration timeout)
            throws ScenarioFailure, IOException, InterruptedException {
        await(
                "more than " + count + " completed checkpoint(s)",
                timeout,
                () -> completedCheckpoints() > count ? Optional.of(true) : Optional.empty());
    }

    /** Fails if a process which is not intentionally down exits during the given time. */
    void observe(Duration time) throws ScenarioFailure, IOException, InterruptedException {
        final long end = System.currentTimeMillis() + time.toMillis();
        while (System.currentTimeMillis() < end) {
            assertNoUnexpectedExit();
            Thread.sleep(1_000);
        }
        assertNoUnexpectedExit();
    }

    /** Waits until every standby has been up for the given time. */
    void awaitStandbysAged(ProcessSpec leader, Duration age)
            throws ScenarioFailure, IOException, InterruptedException {
        await(
                "every standby JobManager to be up for " + age.toSeconds() + "s",
                age.plusMinutes(2),
                () -> {
                    for (ProcessSpec jobManager : jobManagers) {
                        if (jobManager.equals(leader) || down.contains(jobManager.name())) {
                            continue;
                        }
                        if (ageMillis(jobManager) < age.toMillis()) {
                            return Optional.empty();
                        }
                    }
                    return Optional.of(true);
                });
    }

    // ------------------------------------------------------------------------------------------
    // the job
    // ------------------------------------------------------------------------------------------

    /**
     * Starts a job which runs forever and has state: a streaming example of the distribution,
     * uploaded to the REST endpoint of the leader and run there. An IOException means that the
     * leader is not ready yet.
     */
    void submitJob(ProcessSpec leader) throws IOException {
        final Path jar =
                config.flinkHome()
                        .resolve("examples")
                        .resolve("streaming")
                        .resolve("StateMachineExample.jar");
        final Rest.Response uploaded =
                rest.postFile(leader.restHostPort(), "/jars/upload", "jarfile", jar);
        if (!uploaded.ok()) {
            throw new IOException(
                    leader.name()
                            + " did not accept the upload of "
                            + jar
                            + ": "
                            + uploaded.describe());
        }
        final String filename = uploaded.json().path("filename").asText();
        final String jarId = filename.substring(filename.lastIndexOf('/') + 1);
        final Rest.Response started =
                rest.postJson(
                        leader.restHostPort(), "/jars/" + jarId + "/run", "{\"parallelism\":1}");
        if (!started.ok()) {
            throw new IOException(
                    leader.name() + " did not run " + jarId + ": " + started.describe());
        }
        jobId = started.json().path("jobid").asText();
    }

    String jobId() {
        return jobId;
    }
}
