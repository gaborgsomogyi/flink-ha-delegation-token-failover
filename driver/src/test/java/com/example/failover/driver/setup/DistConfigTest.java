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

package com.example.failover.driver.setup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class DistConfigTest {

    private static final String JAVA_OPTS =
            "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED"
                    + " --add-opens=java.base/java.util=ALL-UNNAMED"
                    + " -Dio.netty.tryReflectionSetAccessible=true";

    /** The shape of the config.yaml which a Flink distribution ships. */
    private static final List<String> SHIPPED =
            List.of(
                    "################################################################",
                    "#  Licensed to the Apache Software Foundation (ASF) under one",
                    "################################################################",
                    "",
                    "# These parameters are required for Java 17 support.",
                    "# They can be safely removed when using Java 8/11.",
                    "env:",
                    "  java:",
                    "    opts:",
                    "      all: " + JAVA_OPTS,
                    "#  # log4j 2 configuration",
                    "#  log:",
                    "#    level: TRACE",
                    "jobmanager:",
                    "  # The host interface the JobManager will bind to: by default localhost",
                    "  bind-host: localhost",
                    "  rpc:",
                    "    address: localhost",
                    "    port: 6123",
                    "  memory:",
                    "    process:",
                    "      size: 1600m",
                    "taskmanager:",
                    "  numberOfTaskSlots: 1",
                    "rest:",
                    "  address: localhost",
                    "  # bind-address: localhost");

    @TempDir Path tempDir;

    @Test
    void readsTheNestedOptionsOfTheShippedFile() {
        assertThat(DistConfig.envOptions(SHIPPED))
                .containsExactly(entry("env.java.opts.all", JAVA_OPTS));
    }

    @Test
    void readsFlatOptionsAndIgnoresTheOthers() {
        assertThat(
                        DistConfig.envOptions(
                                List.of(
                                        "jobmanager.rpc.address: jm1",
                                        "env.java.opts.all: -Dfoo=bar",
                                        "rest.port: 8081")))
                .containsExactly(entry("env.java.opts.all", "-Dfoo=bar"));
    }

    @Test
    void readsSeveralNestedOptionsInFileOrder() {
        assertThat(
                        DistConfig.envOptions(
                                List.of(
                                        "env:",
                                        "  java:",
                                        "    opts:",
                                        "      jobmanager: -Da",
                                        "      taskmanager: -Db",
                                        "    default-opts:",
                                        "      all: -Dc")))
                .containsExactly(
                        entry("env.java.opts.jobmanager", "-Da"),
                        entry("env.java.opts.taskmanager", "-Db"),
                        entry("env.java.default-opts.all", "-Dc"));
    }

    @Test
    void aBlockAfterTheEnvBlockDoesNotInheritItsPath() {
        assertThat(
                        DistConfig.envOptions(
                                List.of(
                                        "env:",
                                        "  java:",
                                        "    opts:",
                                        "      all: -Da",
                                        "java:",
                                        "  opts:",
                                        "    all: -Dnot-an-env-option")))
                .containsExactly(entry("env.java.opts.all", "-Da"));
    }

    @Test
    void ignoresCommentedOutOptions() {
        assertThat(
                        DistConfig.envOptions(
                                List.of(
                                        "# env.java.opts.all: -Dx",
                                        "#env:",
                                        "#  java:",
                                        "#    opts:",
                                        "#      all: -Dy")))
                .isEmpty();
    }

    @Test
    void aCommentInsideTheBlockDoesNotEndIt() {
        assertThat(
                        DistConfig.envOptions(
                                List.of(
                                        "env:",
                                        "  java:",
                                        "    opts:",
                                        "#     all: -Dcommented-out",
                                        "      jobmanager: -Da",
                                        "# a comment at the start of the line",
                                        "      taskmanager: -Db")))
                .containsExactly(
                        entry("env.java.opts.jobmanager", "-Da"),
                        entry("env.java.opts.taskmanager", "-Db"));
    }

    @Test
    void keepsTheValueAsWritten() {
        assertThat(
                        DistConfig.envOptions(
                                List.of(
                                        "env.java.opts.all: \"-Da -Db\"",
                                        "env.java.opts.jobmanager: -Dkey=a:b")))
                .containsExactly(
                        entry("env.java.opts.all", "\"-Da -Db\""),
                        entry("env.java.opts.jobmanager", "-Dkey=a:b"));
    }

    @Test
    void aFileWithoutEnvOptionsGivesNone() {
        assertThat(DistConfig.envOptions(List.of("jobmanager.rpc.address: jm1"))).isEmpty();
    }

    @Test
    void readsAFileFromDisk() throws IOException {
        final Path file = tempDir.resolve("config.yaml");
        Files.write(file, SHIPPED);

        assertThat(DistConfig.envOptions(file))
                .containsExactly(entry("env.java.opts.all", JAVA_OPTS));
    }

    @Test
    void aMissingFileGivesNoOptions() throws IOException {
        assertThat(DistConfig.envOptions(tempDir.resolve("config.yaml"))).isEmpty();
    }
}
