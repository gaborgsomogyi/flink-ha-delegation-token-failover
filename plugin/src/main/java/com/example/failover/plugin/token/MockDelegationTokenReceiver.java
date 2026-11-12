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
import org.apache.flink.core.security.token.DelegationTokenReceiver;

import com.example.failover.plugin.audit.Audit;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Receiver of the mock tokens, in the JobManager and in the TaskManager. It keeps the token in a
 * static field like the S3 receivers of Flink do, with no expiry handling, and audits every update.
 */
public class MockDelegationTokenReceiver implements DelegationTokenReceiver {

    /**
     * Prefix of the name of the control file which makes the receiver of the process with the name
     * that follows drop every token update, like a process which the new leader does not supply
     * with tokens. The driver creates it next to the audit files.
     */
    static final String DROP_TOKENS_FILE_PREFIX = "drop-tokens-";

    @Override
    public String serviceName() {
        return MockDelegationTokenProvider.SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {
        Audit.log("RECEIVER_INIT", "");
        Sampler.ensureStarted();
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) {
        final MockToken token = MockToken.fromBytes(tokens);
        if (dropsTokens()) {
            Audit.log(
                    "UPDATE_DROPPED",
                    "token="
                            + token.id()
                            + " reason=the delivery of tokens to this process is switched off");
            return;
        }
        HeldToken.set(token);
        Audit.log(
                "UPDATE",
                "token="
                        + token.id()
                        + " issuedAt="
                        + token.issuedAtMillis()
                        + " validUntil="
                        + token.validUntilMillis());
    }

    private static boolean dropsTokens() {
        final Path directory = Audit.directory();
        return directory != null
                && Files.exists(directory.resolve(DROP_TOKENS_FILE_PREFIX + Audit.processName()));
    }
}
