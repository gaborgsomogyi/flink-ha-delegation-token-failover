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

import org.apache.flink.configuration.Configuration;

import com.example.failover.plugin.testing.AuditSandbox;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class MockDelegationTokenReceiverTest {

    @TempDir File tempDir;

    private AuditSandbox audit;

    @BeforeEach
    void setUp() throws IOException {
        audit = AuditSandbox.open(tempDir);
    }

    @AfterEach
    void tearDown() {
        audit.close();
    }

    @Test
    void receiverInitAuditsItselfAndStartsTheSampler() throws Exception {
        try {
            new MockDelegationTokenReceiver().init(new Configuration());

            assertThat(audit.lines()).anyMatch(line -> line.endsWith("\tRECEIVER_INIT\t"));
            assertThat(Sampler.isRunning()).isTrue();
        } finally {
            Sampler.stop();
        }
        assertThat(Sampler.isRunning()).isFalse();
    }

    @Test
    void receiverKeepsTheTokenAndAuditsTheUpdate() throws Exception {
        final MockToken token = new MockToken("jm2-4", 10L, 40L);

        new MockDelegationTokenReceiver().onNewTokensObtained(token.toBytes());

        assertThat(HeldToken.get()).isEqualTo(token);
        assertThat(audit.lines()).hasSize(1);
        assertThat(audit.lines().get(0))
                .endsWith("\tUPDATE\ttoken=jm2-4 issuedAt=10 validUntil=40");
    }

    @Test
    void receiverDropsTheTokensWhileTheDriverSwitchedTheirDeliveryOff() throws Exception {
        final MockToken held = new MockToken("jm2-1", 10L, 40L);
        new MockDelegationTokenReceiver().onNewTokensObtained(held.toBytes());
        final File switchedOff =
                new File(
                        audit.directory(),
                        MockDelegationTokenReceiver.DROP_TOKENS_FILE_PREFIX + AuditSandbox.PROCESS);
        assertThat(switchedOff.createNewFile()).isTrue();

        new MockDelegationTokenReceiver()
                .onNewTokensObtained(new MockToken("jm2-2", 20L, 50L).toBytes());

        assertThat(HeldToken.get()).isEqualTo(held);
        assertThat(audit.lines().get(audit.lines().size() - 1))
                .contains("\tUPDATE_DROPPED\t")
                .contains("token=jm2-2");

        assertThat(switchedOff.delete()).isTrue();
        final MockToken renewed = new MockToken("jm2-3", 30L, 60L);
        new MockDelegationTokenReceiver().onNewTokensObtained(renewed.toBytes());
        assertThat(HeldToken.get()).isEqualTo(renewed);
    }

    @Test
    void theSwitchOfAnotherProcessDoesNotMatter() throws Exception {
        final File otherProcess =
                new File(
                        audit.directory(),
                        MockDelegationTokenReceiver.DROP_TOKENS_FILE_PREFIX + "tm9");
        assertThat(otherProcess.createNewFile()).isTrue();
        final MockToken token = new MockToken("jm2-5", 10L, 40L);

        new MockDelegationTokenReceiver().onNewTokensObtained(token.toBytes());

        assertThat(HeldToken.get()).isEqualTo(token);
    }
}
