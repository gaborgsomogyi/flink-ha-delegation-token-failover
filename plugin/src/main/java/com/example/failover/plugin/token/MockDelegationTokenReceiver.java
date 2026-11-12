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

/**
 * Receiver of the mock tokens, in the JobManager and in the TaskManager. It keeps the token in a
 * static field like the S3 receivers of Flink do, with no expiry handling, and audits every update.
 */
public class MockDelegationTokenReceiver implements DelegationTokenReceiver {

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
}
