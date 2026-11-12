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

<p align="center">
  <img src="docs/overview.svg" width="100%" alt="Overview of the test. 1: a real Flink HA cluster in Docker with ZooKeeper, a leader and two standby JobManagers, two TaskManagers, and a shared storage behind tokenfs:// which rejects expired tokens, used by all five processes. 2: tokens live 30 seconds, the leader renews its token every 15 seconds, a standby of today's Flink never does and holds an expired token. 3: the leader is killed; today the new leader reads the storage with an expired token, the call is rejected and the JobManager dies, with the fix the token is fresh and the job recovers. The driver then checks for stale tokens, rejected calls, fatal errors, unexpected exits, renewal cadence, whether every TaskManager got tokens and used the storage after the failover, and timeouts, and exits with 0 (passed) or 1 (failed).">
</p>

An endurance and regression test for the delegation token handling of Apache Flink in a
high-availability setup. It starts a real cluster in Docker (ZooKeeper, several JobManagers,
TaskManagers), keeps mock tokens expiring and being renewed, kills the leader JobManager over and
over, and asserts on facts that were written down by the processes themselves.

It exists because the problems it looks for do not show up in unit tests. They need several JVMs,
a leader election, and tokens that really expire:

* A standby JobManager never renews its own delegation tokens, so after one token lifetime it
  holds an expired token.
* The new leader then reads the shared HA storage. If its own token is still expired at that
  moment, the access is rejected and the JobManager dies with a fatal error. With every standby
  aged the same way, the next candidate can die too, until no JobManager is left.

## What it does

The picture above is the whole story: start the cluster, let the tokens expire, kill the leader,
and see whether the next JobManager still has a valid token. In detail:

* `plugin/` is a Flink plugin with a mock token provider, a mock token receiver and a file system
  for the scheme `tokenfs://`. The tokens expire after a configurable lifetime. `tokenfs` is a
  local file system which rejects every call while the token held by its process is missing or
  expired, the way S3 rejects expired session credentials. The receiver keeps the token in a
  static field, like the S3 receivers of Flink do, and every request reads it from there. The HA
  storage (`high-availability.storageDir`) and the checkpoints (`state.checkpoints.dir`) are on
  `tokenfs://`, in a directory which all containers share.
* `driver/` starts the containers with Docker Compose from a Flink distribution that you built,
  submits a job with state, runs a scenario and checks the facts afterwards. It exits with 0 if
  every fact held and with 1 otherwise.

Every process writes an audit file with one line per event: a token obtained, a token received, a
call of the file system accepted or rejected, a token found expired. The driver asserts on these
files and on the logs, it never assumes that something happened:

| Kind | Meaning |
|---|---|
| `STALE` | a process held an expired token, sampled once per second |
| `VIOLATION` | `tokenfs` rejected a call because the token was missing or expired |
| `FATAL` | a Flink process logged a fatal error (with its root cause) |
| `PROCESS_EXIT` | a process exited that the driver did not kill |
| `UPDATE_CADENCE` | a process received far fewer or far more token updates than its renewal period says |
| `NO_UPDATE` | a TaskManager received no token update between the failover and the end of the settle time |
| `NO_ACCESS` | a TaskManager did not use the file system, with a call it accepted, in the same time |
| `TIMEOUT` | no leader, the job not running, or no new checkpoint within the time limit |

After every failover the driver also requires that a new leader is elected, that the job is
`RUNNING` again, and that another checkpoint completes after the settle time.

The TaskManagers are part of this, they use the file system as well:

* The job runs with every slot filled (`--parallelism`, 4 by default: 2 TaskManagers with 2 slots),
  so every TaskManager runs a part of it, and the state of every checkpoint goes to files
  (`execution.checkpointing.data-inline-threshold: 0 b`). Every TaskManager writes through
  `tokenfs://` for every checkpoint and reads its state back after every failover. `NO_ACCESS`
  catches a TaskManager that does not.
* A TaskManager gets its tokens from the leader only. The settle time is longer than a token
  lives (token lifetime plus one renewal period, 45 s by default), so a TaskManager which the new
  leader did not supply with tokens holds an expired one by the end of it, and `NO_UPDATE`,
  `STALE` or a rejected call (`VIOLATION`) tells.

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
| `aged-standby` (default) | Wait until every standby has been up for longer than two token lifetimes, kill the leader, wait for the new leader and the job, watch the cluster for the settle time. This is the scenario of the two problems above, it takes about five minutes. |
| `rolling` | `aged-standby` in a loop for `--duration` (default 10m): the killed JobManager is started again as a new standby after each failover, and the next failover follows after `--failover-interval`. |
| `zk-blip` | Stop ZooKeeper for 12 seconds, longer than the session timeout, so that every JobManager loses the leadership and one of the same, still running JVMs gets it again. |

`--help` lists all options: number of JobManagers and TaskManagers, parallelism of the job, token
lifetime, renewal ratio, standby age, settle time, images, memory and ports.

## Reading a report

On a Flink without the fix, the aged standbys are reported as `STALE` in every run. Whether they
also die when they take over depends on a race between their own token refresh and the first access
of the new leader to the shared storage: in some runs one of them or both die, in others none.
This is a shortened report of a run in which the leader was killed and both standbys died one
after the other:

```
failovers: 0

token updates received per process: {jm2=6, jm3=2, jm1=2, tm2=6, tm1=6}
file system calls accepted per process: {jm2=514, tm1=137, tm2=137}

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
start and one when they took over. The TaskManagers are fine: both used the file system as usual
until the leader was killed (137 accepted calls each) and kept receiving tokens.

With the fix, every JobManager renews its tokens, also as a standby, and nothing breaks:

```
failovers: 1
  #1 killed jm1, new leader jm3 after 6s, job RUNNING after 6s, checkpoint after 6s, ...

token updates received per process: {jm1=5, jm3=9, jm2=9, tm2=10, tm1=10}
file system calls accepted per process: {jm1=499, tm1=233, tm2=233, jm3=372}

RESULT: PASSED
```

## Layout

```
plugin/     the Flink plugin, loaded by every JobManager and TaskManager
  audit       Audit: one line per event in <process>.audit
  token       the mock token service: MockToken, provider, receiver, HeldToken, Sampler
  fs          the tokenfs:// file system, rejects calls with a missing or expired token
driver/     the command line driver
  (root)      Main: parse the options, run, report, exit code
  config      RunConfig: the options of a run
  setup       what is generated before the containers start: processes, Flink config, compose file
  client      Docker (the CLI) and Rest (the Flink REST API)
  cluster     Cluster: start, kill and observe the processes; LeaderTracker: who leads
  scenario    Scenarios (aged-standby, zk-blip, rolling) and their results
  evidence    what the processes recorded: audit events, log entries, causes of an error
  verify      the facts which must hold: Checks, Violation, ScenarioFailure, Incarnation
  report      Report: the text report
run.sh      builds if needed and starts the driver
```

The packages depend on each other in one direction only. From the bottom: `config` and `evidence`
depend on nothing, `setup` uses `config`, `verify` and `client` use `evidence`, `cluster` uses all of
these, `scenario` runs on a `cluster`, `report` renders what a `scenario` and the checks found, and
`Main` wires it together. The plugin has the same shape: `fs` uses `token`, both use `audit`.

`mvn test` runs the unit tests of both modules, each in the package of the class it tests. They
need no Docker: they cover the plugin, the
parsing of options, the generated configuration, the REST client (against a local HTTP server), the
parsing of logs and audit files, and every check of the driver as a function of recorded facts.
Starting the cluster and the scenarios are only covered by running them. The code is formatted
with `mvn -Pformat spotless:apply` (the profile keeps the formatter out of the default build).

The mock token service has a manual switch: while the file `audit/obtain-fails` exists in the run
directory (use `--keep` to be able to create it), every token request of the provider fails and
`OBTAIN_FAILED` is audited. No scenario uses it yet.

## Showing that the checks notice

A test which cannot fail proves nothing, so two options break a TaskManager on purpose. Run them
on a distribution which passes `aged-standby`: if they do not fail, the checks are blind for the
TaskManagers.

* `--drop-tokens-of tm1` makes the process `tm1` drop every token update from the start of each
  failover on (`UPDATE_DROPPED` is audited), as if the new leader did not supply it with tokens.
  The token of `tm1` expires, its next call of the file system is rejected, the checkpoints stop.
  On the fixed build the report ends like this (shortened):

  ```
  RESULT: FAILED, 4 violation(s)
    [TIMEOUT] gave up after 120s waiting for more than 9 completed checkpoint(s)
    [STALE] tm1: the process held an expired token in 150 sample(s), ...
    [VIOLATION] tm1: the token file system rejected 72 call(s), first: {op=create,
       path=tokenfs:/data/checkpoints/.../chk-44/..., token=jm1-5, reason=expired, ...}
    [NO_UPDATE] tm1: received no token update in the 52s from the start of the failover ...
  ```

* `--parallelism 1` lets only one TaskManager run the job, the other one never uses the file
  system. `[NO_ACCESS] tm1: did not use the file system in the 52s ...` names it, and
  `file system calls accepted per process` has no entry for `tm1`.

## Limits

* The tokens are mocks and the file system is a local directory. It reproduces what happens when
  a token expires, not the behavior of a real service.
* One host, one Docker network. Network partitions other than a stopped ZooKeeper are not
  simulated.
* Every accepted call of the file system is one line in the audit file of its process, about 1 MB
  for ten minutes of `rolling`. A run of many hours writes tens of megabytes.
* The leader is found by the log lines of its ResourceManager service, because the REST endpoint of
  a standby is not a reliable witness.

## License

Apache License 2.0, see [LICENSE](LICENSE).
