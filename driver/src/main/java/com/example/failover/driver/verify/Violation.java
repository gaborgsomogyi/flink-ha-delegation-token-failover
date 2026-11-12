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

package com.example.failover.driver.verify;

import java.time.Instant;

/** A fact which contradicts an expectation of the test. */
public record Violation(String kind, String process, long millis, String message) {

    @Override
    public String toString() {
        final String when = millis > 0 ? Instant.ofEpochMilli(millis).toString() + " " : "";
        final String who = process == null || process.isEmpty() ? "" : process + ": ";
        return "[" + kind + "] " + when + who + message;
    }
}
