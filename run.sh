#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the plugin and the driver if their sources changed, then starts the driver.
# Every argument is passed on to the driver, run it with --help for the options.
#
#   MVN       the Maven executable, default: mvn
#   MVN_ARGS  extra Maven arguments, for example -o for an offline build

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
driver_jar="$root/driver/target/failover-driver.jar"
plugin_jar="$root/plugin/target/flink-ha-delegation-token-failover-plugin.jar"

stale=false
for jar in "$driver_jar" "$plugin_jar"; do
  if [[ ! -f "$jar" ]] ||
    [[ -n "$(find "$root/pom.xml" "$root/plugin/pom.xml" "$root/driver/pom.xml" \
      "$root/plugin/src" "$root/driver/src" -type f -newer "$jar")" ]]; then
    stale=true
  fi
done

if [[ "$stale" == true ]]; then
  # MVN_ARGS is split on purpose, it holds several arguments
  # shellcheck disable=SC2086
  "${MVN:-mvn}" -f "$root/pom.xml" -q -DskipTests ${MVN_ARGS:-} package
fi

# The options given by the caller come last, so they win over these defaults.
exec java -jar "$driver_jar" --plugin-jar "$plugin_jar" --work-dir "$root/runs" "$@"
