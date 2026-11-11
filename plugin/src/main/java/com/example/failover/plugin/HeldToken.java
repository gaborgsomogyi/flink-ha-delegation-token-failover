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

package com.example.failover.plugin;

/**
 * The token the receiver of this process currently holds. A static field on purpose: the S3
 * receivers of Flink keep their credentials the same way, and the file system reads them on every
 * request. Provider, receiver and file system live in one plugin, so they share this field.
 */
final class HeldToken {

    private static volatile MockToken current;

    private HeldToken() {}

    static MockToken get() {
        return current;
    }

    static void set(MockToken token) {
        current = token;
    }

    /** Used by tests. */
    static void clear() {
        current = null;
    }
}
