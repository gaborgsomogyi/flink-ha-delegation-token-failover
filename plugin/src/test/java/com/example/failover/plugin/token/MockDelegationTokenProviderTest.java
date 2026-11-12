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
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockDelegationTokenProviderTest {

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
    void providerIssuesATokenWhichIsValidForTheConfiguredLifetime() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(MockDelegationTokenProvider.LIFETIME, Duration.ofSeconds(7));
        final MockDelegationTokenProvider provider = new MockDelegationTokenProvider();
        provider.init(configuration);

        final long before = System.currentTimeMillis();
        final var obtained = provider.obtainDelegationTokens();
        final long after = System.currentTimeMillis();

        final MockToken token = MockToken.fromBytes(obtained.getTokens());
        assertThat(token.id()).isEqualTo("test-process-1");
        assertThat(token.issuedAtMillis()).isBetween(before, after);
        assertThat(token.validUntilMillis() - token.issuedAtMillis()).isEqualTo(7_000L);
        assertThat(obtained.getValidUntil()).contains(token.validUntilMillis());
        assertThat(provider.obtainDelegationTokens().getTokens())
                .isNotEqualTo(obtained.getTokens());
        assertThat(audit.lines()).hasSize(3); // PROVIDER_INIT and two OBTAIN
        assertThat(audit.lines().get(1)).contains("\tOBTAIN\t").contains("token=test-process-1");
    }

    @Test
    void providerFailsWhileTheDriverSwitchedTheServiceOff() throws Exception {
        final MockDelegationTokenProvider provider = new MockDelegationTokenProvider();
        provider.init(new Configuration());
        final File switchedOff =
                new File(audit.directory(), MockDelegationTokenProvider.OBTAIN_FAILS_FILE);
        assertThat(switchedOff.createNewFile()).isTrue();

        assertThatThrownBy(provider::obtainDelegationTokens)
                .isInstanceOf(IOException.class)
                .hasMessage("The mock token service is switched off");
        assertThat(audit.lines().get(audit.lines().size() - 1)).contains("\tOBTAIN_FAILED\t");

        assertThat(switchedOff.delete()).isTrue();
        assertThat(provider.obtainDelegationTokens().getTokens()).isNotEmpty();
    }
}
