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

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** The text report of a run. */
public final class Report {

    private Report() {}

    public static String render(
            RunConfig config,
            Workspace workspace,
            List<IterationResult> iterations,
            List<Violation> violations,
            List<String> evidence,
            Map<String, Long> updatesPerProcess) {
        final String eol = System.lineSeparator();
        final StringBuilder text = new StringBuilder();
        text.append("== delegation token failover run ==").append(eol);
        text.append("distribution:   ").append(config.flinkHome()).append(eol);
        text.append("scenario:       ").append(config.scenario()).append(eol);
        text.append("cluster:        ")
                .append(config.jobManagers())
                .append(" JobManagers, ")
                .append(config.taskManagers())
                .append(" TaskManagers")
                .append(eol);
        text.append("mock tokens:    lifetime ")
                .append(config.tokenLifetime().toSeconds())
                .append("s, renewal ratio ")
                .append(config.renewalRatio())
                .append(" (renewed every ")
                .append(config.renewalPeriod().toSeconds())
                .append("s)")
                .append(eol);
        text.append("run directory:  ").append(workspace.runDirectory()).append(eol);
        text.append(eol);

        text.append("failovers: ").append(iterations.size()).append(eol);
        for (IterationResult iteration : iterations) {
            text.append("  #")
                    .append(iteration.index())
                    .append(" ")
                    .append(iteration.what())
                    .append(", new leader ")
                    .append(iteration.newLeader())
                    .append(" after ")
                    .append(iteration.secondsToLeader())
                    .append("s, job RUNNING after ")
                    .append(iteration.secondsToJobRunning())
                    .append("s, checkpoint after ")
                    .append(iteration.secondsToCheckpoint())
                    .append("s, JobManager threads ")
                    .append(iteration.threadCounts())
                    .append(eol);
        }
        text.append(eol);

        text.append("token updates received per process: ").append(updatesPerProcess).append(eol);
        text.append(eol);

        if (violations.isEmpty()) {
            text.append("RESULT: PASSED").append(eol);
        } else {
            text.append("RESULT: FAILED, ")
                    .append(violations.size())
                    .append(" violation(s)")
                    .append(eol);
            for (Violation violation : violations) {
                text.append("  ").append(violation).append(eol);
            }
            for (String item : evidence) {
                text.append(eol).append(item).append(eol);
            }
        }
        return text.toString();
    }

    static String describe(Duration duration) {
        return duration.toMinutes() > 0
                ? duration.toMinutes()
                        + "m"
                        + (duration.toSecondsPart() > 0 ? duration.toSecondsPart() + "s" : "")
                : duration.toSeconds() + "s";
    }
}
