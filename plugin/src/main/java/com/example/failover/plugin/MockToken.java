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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The mock token. It is opaque to Flink: the provider serializes it, the receivers deserialize it.
 * It carries the instant until which the (imaginary) remote service accepts it.
 */
public final class MockToken {

    private static final String SEPARATOR = ";";

    private final String id;
    private final long issuedAtMillis;
    private final long validUntilMillis;

    public MockToken(String id, long issuedAtMillis, long validUntilMillis) {
        this.id = Objects.requireNonNull(id, "id");
        if (id.contains(SEPARATOR)) {
            throw new IllegalArgumentException("The token id must not contain '" + SEPARATOR + "'");
        }
        this.issuedAtMillis = issuedAtMillis;
        this.validUntilMillis = validUntilMillis;
    }

    public String id() {
        return id;
    }

    public long issuedAtMillis() {
        return issuedAtMillis;
    }

    public long validUntilMillis() {
        return validUntilMillis;
    }

    public boolean isValidAt(long nowMillis) {
        return nowMillis < validUntilMillis;
    }

    public byte[] toBytes() {
        return (id + SEPARATOR + issuedAtMillis + SEPARATOR + validUntilMillis)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static MockToken fromBytes(byte[] bytes) {
        final String[] parts = new String(bytes, StandardCharsets.UTF_8).split(SEPARATOR, -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "Not a mock token: " + new String(bytes, StandardCharsets.UTF_8));
        }
        return new MockToken(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MockToken)) {
            return false;
        }
        final MockToken that = (MockToken) o;
        return issuedAtMillis == that.issuedAtMillis
                && validUntilMillis == that.validUntilMillis
                && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, issuedAtMillis, validUntilMillis);
    }

    @Override
    public String toString() {
        return "MockToken{id="
                + id
                + ", issuedAt="
                + issuedAtMillis
                + ", validUntil="
                + validUntilMillis
                + '}';
    }
}
