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

package com.example.failover.plugin.token;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockTokenTest {

    @Test
    void bytesRoundTrip() {
        final MockToken token = new MockToken("jm1-7", 1_000L, 31_000L);

        final MockToken restored = MockToken.fromBytes(token.toBytes());

        assertThat(restored).isEqualTo(token);
        assertThat(restored.id()).isEqualTo("jm1-7");
        assertThat(restored.issuedAtMillis()).isEqualTo(1_000L);
        assertThat(restored.validUntilMillis()).isEqualTo(31_000L);
    }

    @Test
    void validityEndsExactlyAtValidUntil() {
        final MockToken token = new MockToken("t", 0L, 100L);

        assertThat(token.isValidAt(99L)).isTrue();
        assertThat(token.isValidAt(100L)).isFalse();
        assertThat(token.isValidAt(101L)).isFalse();
    }

    @Test
    void garbageIsRejectedWithTheContentInTheMessage() {
        assertThatThrownBy(() -> MockToken.fromBytes("not a token".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a mock token: not a token");
    }

    @Test
    void aTokenWithTooManyPartsIsRejected() {
        assertThatThrownBy(() -> MockToken.fromBytes("a;1;2;3".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a mock token: a;1;2;3");
    }

    @Test
    void idWithTheSeparatorIsRejected() {
        assertThatThrownBy(() -> new MockToken("a;b", 0L, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The token id must not contain ';'");
    }
}
