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

/**
 * One life of one process: from its start to its end. A process which the driver killed and started
 * again has several incarnations. <code>endMillis</code> is {@link Long#MAX_VALUE} while the
 * incarnation is still running.
 */
public record Incarnation(
        String process, long startMillis, long endMillis, boolean killedByDriver) {

    public static final long OPEN = Long.MAX_VALUE;

    boolean isOpen() {
        return endMillis == OPEN;
    }

    long lengthMillis(long nowMillis) {
        return Math.min(endMillis, nowMillis) - startMillis;
    }
}
