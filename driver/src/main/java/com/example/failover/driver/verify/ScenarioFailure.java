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

/** A fact which makes the run fail, found while a scenario was waiting for something. */
public final class ScenarioFailure extends Exception {

    private static final long serialVersionUID = 1L;

    private final transient Violation violation;
    private final String evidence;

    public ScenarioFailure(Violation violation, String evidence) {
        super(violation.toString());
        this.violation = violation;
        this.evidence = evidence;
    }

    public Violation violation() {
        return violation;
    }

    /** For example the tail of the log of the process which died. */
    public String evidence() {
        return evidence;
    }
}
