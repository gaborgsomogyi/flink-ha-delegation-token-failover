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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads what the config.yaml of a Flink distribution must give to the generated config.yaml of
 * every process: the <code>env.*</code> settings, in particular the <code>env.java.opts.all</code>
 * with the <code>--add-opens</code> flags which the JVMs need on Java 17. Without them a job can
 * not even be started, because the DataStream environment fails to initialize.
 */
final class DistConfig {

    private DistConfig() {}

    /** Empty if the file does not exist. */
    static Map<String, String> envOptions(Path configYaml) throws IOException {
        if (!Files.isRegularFile(configYaml)) {
            return Map.of();
        }
        return envOptions(Files.readAllLines(configYaml));
    }

    /**
     * The <code>env.*</code> keys with their values as flat keys, in file order. The values are
     * kept as they are written. The shipped file nests the keys (<code>env:</code>, <code>java:
     * </code>, ...), older ones and users write them flat, both are read. Only the plain <code>
     * key: value</code> lines of such a file are understood, no lists and no block scalars.
     */
    static Map<String, String> envOptions(List<String> lines) {
        final Map<String, String> options = new LinkedHashMap<>();
        final List<Integer> indents = new ArrayList<>();
        final List<String> parents = new ArrayList<>();
        for (String line : lines) {
            final String text = line.strip();
            final int colon = text.indexOf(':');
            if (text.isEmpty() || text.startsWith("#") || colon < 0) {
                continue;
            }
            final int indent = line.length() - line.stripLeading().length();
            while (!indents.isEmpty() && indents.get(indents.size() - 1) >= indent) {
                indents.remove(indents.size() - 1);
                parents.remove(parents.size() - 1);
            }
            final String key = text.substring(0, colon).strip();
            final String value = text.substring(colon + 1).strip();
            if (value.isEmpty()) {
                indents.add(indent);
                parents.add(key);
                continue;
            }
            final String path = parents.isEmpty() ? key : String.join(".", parents) + "." + key;
            if (path.startsWith("env.")) {
                options.put(path, value);
            }
        }
        return options;
    }
}
