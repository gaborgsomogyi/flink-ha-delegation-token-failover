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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RunConfigTest {

    @TempDir Path tempDir;

    private Path distribution;

    @BeforeEach
    void createDistribution() throws IOException {
        distribution = tempDir.resolve("distribution");
        Files.createDirectories(distribution.resolve("bin"));
        Files.createDirectories(distribution.resolve("lib"));
    }

    private RunConfig parse(String... extraArguments) {
        final List<String> arguments =
                new ArrayList<>(List.of("--flink-home", distribution.toString()));
        arguments.addAll(List.of(extraArguments));
        return RunConfig.parse(arguments.toArray(new String[0]));
    }

    @Test
    void theDefaultsAreTheDocumentedOnes() {
        final RunConfig config = parse();

        assertThat(config.flinkHome()).isEqualTo(distribution);
        assertThat(config.scenario()).isEqualTo("aged-standby");
        assertThat(config.jobManagers()).isEqualTo(3);
        assertThat(config.taskManagers()).isEqualTo(2);
        assertThat(config.tokenLifetime()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.renewalRatio()).isEqualTo(0.5);
        assertThat(config.renewalPeriod()).isEqualTo(Duration.ofSeconds(15));
        assertThat(config.standbyAge()).isEqualTo(Duration.ofSeconds(70));
        assertThat(config.failoverInterval()).isEqualTo(Duration.ofSeconds(90));
        assertThat(config.settle()).isEqualTo(Duration.ofSeconds(45));
        assertThat(config.parallelism()).isEqualTo(4);
        assertThat(config.dropTokensOf()).isEmpty();
        assertThat(config.duration()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.keep()).isFalse();
        assertThat(config.restPortBase()).isEqualTo(18081);
    }

    @Test
    void theSettleTimeIsLongerThanATokenLivesUnlessItIsGiven() {
        // the token lifetime plus one renewal period, so that a TaskManager which does not get
        // tokens from the new leader is seen with an expired token
        assertThat(parse("--token-lifetime", "1m").settle()).isEqualTo(Duration.ofSeconds(90));
        assertThat(parse("--token-lifetime", "40s", "--renewal-ratio", "0.25").settle())
                .isEqualTo(Duration.ofSeconds(50));
        assertThat(parse("--settle", "5s").settle()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void theJobFillsEverySlotSoThatEveryTaskManagerRunsAPartOfIt() {
        assertThat(parse("--task-managers", "3").parallelism())
                .isEqualTo(3 * RunConfig.SLOTS_PER_TASK_MANAGER);
        assertThat(parse("--parallelism", "1").parallelism()).isEqualTo(1);
    }

    @Test
    void aProcessCanBeToldToDropItsTokens() {
        assertThat(parse("--drop-tokens-of", "tm2").dropTokensOf()).isEqualTo("tm2");
        assertThat(parse("--drop-tokens-of", "jm3").dropTokensOf()).isEqualTo("jm3");
        assertThatThrownBy(() -> parse("--drop-tokens-of", "tm3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown process: tm3, one of [jm1, jm2, jm3, tm1, tm2]");
    }

    @Test
    void theParallelismMustFitIntoTheSlots() {
        assertThatThrownBy(() -> parse("--parallelism", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The parallelism must be between 1 and 4, the number of slots");
        assertThatThrownBy(() -> parse("--parallelism", "5"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The parallelism must be between 1 and 4, the number of slots");
    }

    @Test
    void theStandbyAgeFollowsTheTokenLifetimeUnlessItIsGiven() {
        assertThat(parse("--token-lifetime", "1m").standbyAge()).isEqualTo(Duration.ofSeconds(130));
        assertThat(parse("--token-lifetime", "1m", "--standby-age", "5s").standbyAge())
                .isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void optionsOverrideTheDefaults() {
        final RunConfig config =
                parse(
                        "--scenario",
                        "rolling",
                        "--duration",
                        "2h",
                        "--job-managers",
                        "4",
                        "--task-managers",
                        "3",
                        "--renewal-ratio",
                        "0.25",
                        "--failover-interval",
                        "45s",
                        "--settle",
                        "5s",
                        "--rest-port-base",
                        "19000",
                        "--keep");

        assertThat(config.scenario()).isEqualTo("rolling");
        assertThat(config.duration()).isEqualTo(Duration.ofHours(2));
        assertThat(config.jobManagers()).isEqualTo(4);
        assertThat(config.taskManagers()).isEqualTo(3);
        assertThat(config.renewalPeriod()).isEqualTo(Duration.ofMillis(7500));
        assertThat(config.failoverInterval()).isEqualTo(Duration.ofSeconds(45));
        assertThat(config.settle()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.restPortBase()).isEqualTo(19000);
        assertThat(config.keep()).isTrue();
    }

    @Test
    void theFlinkHomeIsRequired() {
        assertThatThrownBy(() -> RunConfig.parse(new String[] {"--scenario", "rolling"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("--flink-home is required");
    }

    @Test
    void aDirectoryWhichIsNoDistributionIsRejected() {
        assertThatThrownBy(() -> RunConfig.parse(new String[] {"--flink-home", tempDir.toString()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(tempDir + " is not a Flink distribution (no bin/ or lib/)");
    }

    @Test
    void anUnknownScenarioIsRejected() {
        assertThatThrownBy(() -> parse("--scenario", "chaos"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown scenario: chaos");
    }

    @Test
    void aFailoverNeedsTwoJobManagers() {
        assertThatThrownBy(() -> parse("--job-managers", "1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("At least two JobManagers are needed for a failover");
    }

    @Test
    void theRenewalRatioMustBeBetweenZeroAndOne() {
        assertThatThrownBy(() -> parse("--renewal-ratio", "1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The renewal ratio must be in (0, 1)");
    }

    @Test
    void anOptionWithoutValueIsRejected() {
        assertThatThrownBy(() -> parse("--settle"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown or incomplete option: --settle");
    }

    @Test
    void durationsAreWrittenWithAUnitOrAsIso8601() {
        assertThat(RunConfig.parseDuration("30s")).isEqualTo(Duration.ofSeconds(30));
        assertThat(RunConfig.parseDuration("5m")).isEqualTo(Duration.ofMinutes(5));
        assertThat(RunConfig.parseDuration("2h")).isEqualTo(Duration.ofHours(2));
        assertThat(RunConfig.parseDuration("PT1M30S")).isEqualTo(Duration.ofSeconds(90));
        assertThatThrownBy(() -> RunConfig.parseDuration("12x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a duration (use 30s, 5m or 2h): 12x");
    }
}
