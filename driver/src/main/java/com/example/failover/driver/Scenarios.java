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
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** The scenarios. Each one fails fast with a {@link ScenarioFailure} on the first broken fact. */
final class Scenarios {

    private static final Duration LEADER_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration CHECKPOINT_TIMEOUT = Duration.ofSeconds(120);

    private Scenarios() {}

    static void run(
            String scenario, Cluster cluster, RunConfig config, List<IterationResult> results)
            throws ScenarioFailure, IOException, InterruptedException {
        bootstrap(cluster);
        switch (scenario) {
            case "aged-standby":
                results.add(failoverOnce(cluster, config, 1));
                break;
            case "rolling":
                rolling(cluster, config, results);
                break;
            case "zk-blip":
                results.add(zookeeperBlip(cluster, config, 1));
                break;
            default:
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    /** Waits for the first leader, starts the job and waits until it checkpoints. */
    static void bootstrap(Cluster cluster)
            throws ScenarioFailure, IOException, InterruptedException {
        final ProcessSpec leader = cluster.awaitLeader(Set.of(), Duration.ofMinutes(3));
        // The leader is granted leadership before its Dispatcher and REST endpoint are ready.
        IOException lastFailure = null;
        for (int attempt = 1; attempt <= 12; attempt++) {
            try {
                cluster.submitJob(leader);
                lastFailure = null;
                break;
            } catch (IOException e) {
                lastFailure = e;
                cluster.assertNoUnexpectedExit();
                Thread.sleep(5_000);
            }
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        cluster.awaitJobRunning(JOB_TIMEOUT);
        cluster.awaitCheckpointsAbove(0, CHECKPOINT_TIMEOUT);
    }

    private static void rolling(Cluster cluster, RunConfig config, List<IterationResult> results)
            throws ScenarioFailure, IOException, InterruptedException {
        final long end = System.currentTimeMillis() + config.duration().toMillis();
        int index = 1;
        do {
            final long started = System.currentTimeMillis();
            results.add(failoverOnce(cluster, config, index++));
            final long next = started + config.failoverInterval().toMillis();
            while (System.currentTimeMillis() < Math.min(next, end)) {
                cluster.assertNoUnexpectedExit();
                Thread.sleep(1_000);
            }
        } while (System.currentTimeMillis() < end);
    }

    /**
     * Kills the leader after every standby has outlived the token lifetime, waits until another
     * JobManager leads and the job checkpoints again, and starts the killed one as a new standby.
     */
    static IterationResult failoverOnce(Cluster cluster, RunConfig config, int index)
            throws ScenarioFailure, IOException, InterruptedException {
        ProcessSpec leader = cluster.awaitLeader(Set.of(), LEADER_TIMEOUT);
        cluster.awaitStandbysAged(leader, config.standbyAge());
        leader = cluster.awaitLeader(Set.of(), LEADER_TIMEOUT);

        final long killedAt = System.currentTimeMillis();
        cluster.kill(leader);
        final ProcessSpec newLeader = cluster.awaitLeader(Set.of(leader.name()), LEADER_TIMEOUT);
        final long leaderAt = System.currentTimeMillis();
        cluster.awaitJobRunning(JOB_TIMEOUT);
        final long runningAt = System.currentTimeMillis();
        // The checkpoint statistics belong to the JobMaster, so they restart with the new leader.
        cluster.awaitCheckpointsAbove(0, CHECKPOINT_TIMEOUT);
        final long checkpointAt = System.currentTimeMillis();
        settle(cluster, config);

        final Map<String, Integer> threads = threadCounts(cluster, leader);
        cluster.startAgain(leader);
        return new IterationResult(
                index,
                "killed " + leader.name(),
                newLeader.name(),
                seconds(killedAt, leaderAt),
                seconds(killedAt, runningAt),
                seconds(killedAt, checkpointAt),
                threads);
    }

    /**
     * Takes ZooKeeper away for longer than the session timeout. Every JobManager loses the
     * leadership and one of the same, still running JVMs is granted it again.
     */
    static IterationResult zookeeperBlip(Cluster cluster, RunConfig config, int index)
            throws ScenarioFailure, IOException, InterruptedException {
        final ProcessSpec leader = cluster.awaitLeader(Set.of(), LEADER_TIMEOUT);
        final int grantsBefore = cluster.totalLeadershipGrants();

        final long blipAt = System.currentTimeMillis();
        cluster.blinkZookeeper(Duration.ofSeconds(12));
        cluster.await(
                "a new leadership grant after ZooKeeper came back",
                LEADER_TIMEOUT.plusSeconds(60),
                () -> {
                    try {
                        return cluster.totalLeadershipGrants() > grantsBefore
                                        && cluster.currentLeader(Set.of()).isPresent()
                                ? Optional.of(true)
                                : Optional.empty();
                    } catch (IOException e) {
                        return Optional.empty();
                    }
                });
        final ProcessSpec newLeader = cluster.awaitLeader(Set.of(), LEADER_TIMEOUT);
        final long leaderAt = System.currentTimeMillis();
        cluster.awaitJobRunning(JOB_TIMEOUT);
        final long runningAt = System.currentTimeMillis();
        cluster.awaitCheckpointsAbove(0, CHECKPOINT_TIMEOUT);
        final long checkpointAt = System.currentTimeMillis();
        settle(cluster, config);

        return new IterationResult(
                index,
                "ZooKeeper was away, leader before: " + leader.name(),
                newLeader.name(),
                seconds(blipAt, leaderAt),
                seconds(blipAt, runningAt),
                seconds(blipAt, checkpointAt),
                threadCounts(cluster, null));
    }

    /**
     * Watches the cluster after a failover. A process which exits is a failure, and the job has to
     * complete another checkpoint, which shows that it still runs and still reaches the file
     * system.
     */
    private static void settle(Cluster cluster, RunConfig config)
            throws ScenarioFailure, IOException, InterruptedException {
        final int checkpoints = Math.max(0, cluster.completedCheckpoints());
        cluster.observe(config.settle());
        cluster.awaitCheckpointsAbove(checkpoints, CHECKPOINT_TIMEOUT);
    }

    private static Map<String, Integer> threadCounts(Cluster cluster, ProcessSpec killed) {
        final Map<String, Integer> counts = new LinkedHashMap<>();
        for (ProcessSpec jobManager : cluster.jobManagers()) {
            if (!jobManager.equals(killed)) {
                counts.put(jobManager.name(), cluster.threadCount(jobManager));
            }
        }
        return counts;
    }

    private static long seconds(long from, long to) {
        return Duration.ofMillis(to - from).toSeconds();
    }
}
