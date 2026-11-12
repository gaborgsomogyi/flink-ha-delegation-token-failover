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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;

import com.example.failover.plugin.audit.Audit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provider of the mock token service. Every call returns a fresh token which is valid for the
 * configured lifetime, and reports that instant as the renewal date, so that Flink renews at the
 * configured ratio of it.
 */
public class MockDelegationTokenProvider implements DelegationTokenProvider {

    public static final String SERVICE_NAME = "mock-expiring";

    /** Name of the control file which makes the next obtains fail while it exists. */
    static final String OBTAIN_FAILS_FILE = "obtain-fails";

    static final ConfigOption<Duration> LIFETIME =
            ConfigOptions.key(CONFIG_PREFIX + "." + SERVICE_NAME + ".lifetime")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("How long a token issued by the mock service is valid.");

    private final AtomicLong counter = new AtomicLong();

    private long lifetimeMillis = LIFETIME.defaultValue().toMillis();

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {
        lifetimeMillis = configuration.get(LIFETIME).toMillis();
        Audit.log("PROVIDER_INIT", "lifetimeMs=" + lifetimeMillis);
    }

    @Override
    public boolean delegationTokensRequired() {
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        failIfTheServiceIsSwitchedOff();

        final long now = System.currentTimeMillis();
        final MockToken token =
                new MockToken(
                        Audit.processName() + "-" + counter.incrementAndGet(),
                        now,
                        now + lifetimeMillis);
        Audit.log(
                "OBTAIN",
                "token="
                        + token.id()
                        + " issuedAt="
                        + token.issuedAtMillis()
                        + " validUntil="
                        + token.validUntilMillis());
        return new ObtainedDelegationTokens(token.toBytes(), Optional.of(token.validUntilMillis()));
    }

    /**
     * The driver switches the mock token service off by creating a file next to the audit files.
     */
    private static void failIfTheServiceIsSwitchedOff() throws IOException {
        final Path directory = Audit.directory();
        if (directory != null && Files.exists(directory.resolve(OBTAIN_FAILS_FILE))) {
            Audit.log("OBTAIN_FAILED", "reason=the mock token service is switched off");
            throw new IOException("The mock token service is switched off");
        }
    }
}
