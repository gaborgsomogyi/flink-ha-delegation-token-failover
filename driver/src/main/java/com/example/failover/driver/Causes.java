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

import java.util.ArrayList;
import java.util.List;

/** Shortens a stack trace to what tells why something failed. */
final class Causes {

    private static final int MAX_LINES = 6;
    private static final int MAX_LINE_LENGTH = 300;
    private static final String CAUSE = "Caused by:";

    private Causes() {}

    /**
     * The first <code>head</code> lines of the text and every line starting with "Caused by:",
     * joined with " | ". At most six lines are kept and the last one is always the root cause.
     * Stack frames are dropped and long lines are cut.
     */
    static String summarize(String text, int head) {
        final List<String> lines = new ArrayList<>();
        int seen = 0;
        for (String line : text.split("\\R")) {
            final String stripped = line.strip();
            if (stripped.isEmpty()) {
                continue;
            }
            if (seen++ < head || stripped.startsWith(CAUSE)) {
                lines.add(
                        stripped.length() > MAX_LINE_LENGTH
                                ? stripped.substring(0, MAX_LINE_LENGTH) + "..."
                                : stripped);
            }
        }
        if (lines.size() > MAX_LINES) {
            final List<String> kept = new ArrayList<>(lines.subList(0, MAX_LINES - 1));
            kept.add(lines.get(lines.size() - 1));
            return String.join(" | ", kept);
        }
        return String.join(" | ", lines);
    }
}
