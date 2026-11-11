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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/** One line of an audit file written by the mock token plugin. */
record AuditEvent(long millis, String process, String event, Map<String, String> details) {

    /** Parses <code>millis TAB process TAB EVENT TAB key=value key=value</code>. */
    static Optional<AuditEvent> parse(String line) {
        final String[] parts = line.split("\t", 4);
        if (parts.length < 3) {
            return Optional.empty();
        }
        final long millis;
        try {
            millis = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return Optional.of(
                new AuditEvent(
                        millis,
                        parts[1],
                        parts[2],
                        parseDetails(parts.length == 4 ? parts[3] : "")));
    }

    /** Words without '=' belong to the previous value ("reason=the service is off"). */
    private static Map<String, String> parseDetails(String text) {
        final Map<String, String> details = new LinkedHashMap<>();
        String lastKey = null;
        for (String word : text.trim().split(" ")) {
            if (word.isEmpty()) {
                continue;
            }
            final int separator = word.indexOf('=');
            if (separator > 0) {
                lastKey = word.substring(0, separator);
                details.put(lastKey, word.substring(separator + 1));
            } else if (lastKey != null) {
                details.put(lastKey, details.get(lastKey) + " " + word);
            }
        }
        return details;
    }

    String detail(String key) {
        return details.getOrDefault(key, "");
    }

    boolean is(String name) {
        return event.equals(name);
    }

    /** Reads all *.audit files of a directory, ordered by time. */
    static List<AuditEvent> readAll(Path auditDirectory) throws IOException {
        final List<AuditEvent> events = new ArrayList<>();
        if (!Files.isDirectory(auditDirectory)) {
            return events;
        }
        try (Stream<Path> files = Files.list(auditDirectory)) {
            for (Path file :
                    (Iterable<Path>) files.filter(f -> f.toString().endsWith(".audit"))::iterator) {
                for (String line : Files.readAllLines(file)) {
                    parse(line).ifPresent(events::add);
                }
            }
        }
        events.sort(Comparator.comparingLong(AuditEvent::millis));
        return events;
    }
}
