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

import com.example.failover.plugin.testing.AuditSandbox;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class SamplerTest {

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
    void samplerAuditsStaleOnlyWhileTheHeldTokenIsExpired() throws Exception {
        HeldToken.set(new MockToken("t-1", 0L, 100L));

        Sampler.sampleOnce(99L);
        assertThat(audit.lines()).isEmpty();

        Sampler.sampleOnce(130L);
        assertThat(audit.lines()).hasSize(1);
        assertThat(audit.lines().get(0)).endsWith("\tSTALE\ttoken=t-1 expiredForMs=30");

        HeldToken.clear();
        Sampler.sampleOnce(500L);
        assertThat(audit.lines()).hasSize(1);
    }
}
