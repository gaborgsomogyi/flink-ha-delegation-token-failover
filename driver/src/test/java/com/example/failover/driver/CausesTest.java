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

package com.example.failover.driver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CausesTest {

    private static String withCauses(String first, int causes) {
        final StringBuilder text = new StringBuilder(first);
        for (int i = 1; i <= causes; i++) {
            text.append("\n\tat com.example.Frame").append(i).append(".run(Frame.java:1)");
            text.append("\nCaused by: cause").append(i);
        }
        return text.toString();
    }

    @Test
    void keepsTheFirstLineAndTheCausesAndDropsTheStackFrames() {
        assertThat(Causes.summarize(withCauses("first", 2), 1))
                .isEqualTo("first | Caused by: cause1 | Caused by: cause2");
    }

    @Test
    void keepsTheRequestedNumberOfLeadingLines() {
        final String text =
                "Fatal error occurred\n"
                        + "java.lang.IllegalStateException: top\n"
                        + "\tat com.example.Frame.run(Frame.java:1)\n"
                        + "Caused by: java.io.IOException: root";

        assertThat(Causes.summarize(text, 2))
                .isEqualTo(
                        "Fatal error occurred | java.lang.IllegalStateException: top"
                                + " | Caused by: java.io.IOException: root");
        assertThat(Causes.summarize(text, 1))
                .isEqualTo("Fatal error occurred | Caused by: java.io.IOException: root");
    }

    @Test
    void keepsExactlySixLinesInFull() {
        assertThat(Causes.summarize(withCauses("first", 5), 1))
                .isEqualTo(
                        "first | Caused by: cause1 | Caused by: cause2 | Caused by: cause3"
                                + " | Caused by: cause4 | Caused by: cause5");
    }

    @Test
    void keepsAtMostSixLinesAndAlwaysTheRootCause() {
        assertThat(Causes.summarize(withCauses("first", 10), 1))
                .isEqualTo(
                        "first | Caused by: cause1 | Caused by: cause2 | Caused by: cause3"
                                + " | Caused by: cause4 | Caused by: cause10");
    }

    @Test
    void cutsALongLine() {
        assertThat(Causes.summarize("x".repeat(400), 1)).isEqualTo("x".repeat(300) + "...");
    }

    @Test
    void doesNotCutALineOfExactlyTheMaximumLength() {
        assertThat(Causes.summarize("x".repeat(300), 1)).isEqualTo("x".repeat(300));
    }

    @Test
    void skipsEmptyLinesAtTheStart() {
        assertThat(Causes.summarize("\n  \nfirst\nCaused by: root", 1))
                .isEqualTo("first | Caused by: root");
    }

    @Test
    void anEmptyTextGivesAnEmptySummary() {
        assertThat(Causes.summarize("", 1)).isEmpty();
    }
}
