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

package com.example.failover.driver.cluster;

/**
 * The time from a disturbance of the cluster (the leader was killed, ZooKeeper went away) to the
 * end of the watch which followed it. In this time the checks expect every TaskManager to receive
 * tokens and to use the file system.
 */
public record Observation(long startMillis, long endMillis) {}
