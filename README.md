<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# flink-ha-delegation-token-failover

An endurance and regression test for the delegation token handling of Apache Flink in a
high-availability setup. It starts a real cluster in Docker (ZooKeeper, several JobManagers,
TaskManagers), keeps mock tokens expiring and being renewed, kills the leader JobManager over and
over, and asserts on facts that were written down by the processes themselves.

It exists because the problems it looks for do not show up in unit tests. They need several JVMs,
a leader election, and tokens that really expire:

* A standby JobManager never renews its own delegation tokens, so after one token lifetime it
  holds an expired token.
* The new leader then reads the shared HA storage with that expired token. The access is rejected,
  the JobManager dies with a fatal error, and with every standby aged the same way the next
  candidate dies too.

## What it does

```
            ZooKeeper
               |
   +-----------+------------+          shared directory /data  (scheme tokenfs://)
   |           |            |                 high-availability.storageDir
  jm1         jm2          jm3                state.checkpoints.dir
 leader      standby      standby
   |
  tm1, tm2  <- the job of the leader runs here
```

* `plugin/` is a Flink plugin with a mock token provider, a mock token receiver and a file system
  for the scheme `tokenfs://`. The tokens expire after a configurable lifetime. `tokenfs` is a
  local file system which rejects every call while the token held by its process is missing or
  expired, the way S3 rejects expired session credentials. The receiver keeps the token in a
  static field, like the S3 receivers of Flink do, and every request reads it from there.
* `driver/` starts the containers with Docker Compose from a Flink distribution that you built,
  submits a job with state, runs a scenario and checks the facts afterwards. It exits with 0 if
  every fact held and with 1 otherwise.

Every process writes an audit file with one line per event. The driver asserts on these files and
on the logs, it never assumes that something happened:

| Kind | Meaning |
|---|---|
| `STALE` | a process held an expired token, sampled once per second |
| `VIOLATION` | `tokenfs` rejected a call because the token was missing or expired |
| `FATAL` | a Flink process logged a fatal error (with its root cause) |
| `PROCESS_EXIT` | a process exited that the driver did not kill |
| `UPDATE_CADENCE` | a process received far fewer or far more token updates than its renewal period says |
| `TIMEOUT` | no leader, the job not running, or no new checkpoint within the time limit |

After every failover the driver also requires that a new leader is elected, that the job is
`RUNNING` again, and that another checkpoint completes after a settle time.

## Requirements

* JDK 17 and Maven 3.9 to build, Docker with Compose v2 to run.
* A Flink distribution of Flink 2.x built for Java 17, for example from the Flink repository:

  ```
  ./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C
  # the distribution is flink-dist/target/flink-<version>-bin/flink-<version>
  ```

  The distribution is mounted read-only into the containers, nothing in it is modified. Its
  `conf/config.yaml` is not used as it is: every process gets a generated one, but the `env.*`
  settings of the distribution (the `--add-opens` flags for Java 17) are carried over. The
  containers run `eclipse-temurin:17-jre`, pass `--runtime-image` if the distribution needs
  another Java.

## Run it

```
./run.sh --flink-home /path/to/flink-2.5-SNAPSHOT --scenario aged-standby
```

`run.sh` builds the plugin and the driver when their sources changed and then starts the driver
with all given arguments. Set `MVN` and `MVN_ARGS` to use another Maven or to pass arguments such
as `-o`. Without `run.sh`:

```
mvn package
java -jar driver/target/failover-driver.jar --flink-home /path/to/flink-2.5-SNAPSHOT
```

Everything a run creates goes to `runs/run-<time>/`: the generated configuration and compose file,
the logs and audit file of each process, the shared data, and `report.txt`. The containers are
removed at the end, unless `--keep` is given.

### Scenarios

| Scenario | What happens |
|---|---|
| `aged-standby` (default) | Wait until every standby has been up for longer than two token lifetimes, kill the leader, wait for the new leader and the job, watch the cluster for the settle time. This is the scenario of the two problems above, it takes about three minutes. |
| `rolling` | `aged-standby` in a loop for `--duration` (default 10m): the killed JobManager is started again as a new standby after each failover, and the next failover follows after `--failover-interval`. |
| `zk-blip` | Stop ZooKeeper for 12 seconds, longer than the session timeout, so that every JobManager loses the leadership and one of the same, still running JVMs gets it again. |

`--help` lists all options: number of JobManagers and TaskManagers, token lifetime, renewal ratio,
standby age, settle time, images, memory and ports.

## Reading a report

On a Flink without the fix, the aged standbys are reported as `STALE` in every run. Whether they
also die when they take over depends on a race between their own token refresh and the first access
of the new leader to the shared storage: in some runs one of them or both die, in others none.
This is a shortened report of a run in which the leader was killed and both standbys died one
after the other:

```
failovers: 0

token updates received per process: {jm2=6, jm1=2, jm3=2, tm2=7, tm1=7}

RESULT: FAILED, 7 violation(s)
  [PROCESS_EXIT] jm1: exited unexpectedly, status=exited, exit code 239 (...)
  [FATAL] jm1: Fatal error occurred in the cluster entrypoint. | ...CompletionException: ...
     Could not retrieve JobResults of globally-terminated jobs from JobResultStore
     | Caused by: java.io.IOException: tokenfs rejected mkdirs
     tokenfs:/data/ha/job-result-store/failover: token jm1-1 is expired since 44273 ms
  [FATAL] jm3: Fatal error occurred in the cluster entrypoint. | ...
  [STALE] jm1: the process held an expired token in 45 sample(s), ...
  [STALE] jm3: the process held an expired token in 45 sample(s), ...
  [VIOLATION] jm1: the token file system rejected 2 call(s), ...
  [VIOLATION] jm3: the token file system rejected 2 call(s), ...
```

Note that jm2, the leader, received six token updates while the standbys received one at their
start and one when they took over.

With the fix, every JobManager renews its tokens, also as a standby, and nothing breaks:

```
failovers: 1
  #1 killed jm1, new leader jm3 after 5s, job RUNNING after 5s, checkpoint after 5s, ...

token updates received per process: {jm1=5, jm3=6, jm2=6, tm1=7, tm2=7}

RESULT: PASSED
```

## Layout

```
plugin/   the Flink plugin: provider, receiver, tokenfs, audit, sampler
driver/   the command line driver
run.sh    builds if needed and starts the driver
```

`mvn test` runs the unit tests of both modules. They need no Docker: they cover the plugin, the
parsing of options, the generated configuration, the REST client (against a local HTTP server), the
parsing of logs and audit files, and every check of the driver as a function of recorded facts.
Starting the cluster and the scenarios are only covered by running them. The code is formatted
with `mvn -Pformat spotless:apply` (the profile keeps the formatter out of the default build).

The mock token service has a manual switch: while the file `audit/obtain-fails` exists in the run
directory (use `--keep` to be able to create it), every token request of the provider fails and
`OBTAIN_FAILED` is audited. No scenario uses it yet.

## Limits

* The tokens are mocks and the file system is a local directory. It reproduces what happens when
  a token expires, not the behavior of a real service.
* One host, one Docker network. Network partitions other than a stopped ZooKeeper are not
  simulated.
* The leader is found by the log lines of its ResourceManager service, because the REST endpoint of
  a standby is not a reliable witness.

## License

Apache License 2.0, see [LICENSE](LICENSE).
