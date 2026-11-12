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

package com.example.failover.driver.client;

import com.example.failover.driver.evidence.Causes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Minimal client of the Flink REST API of the JobManagers, through the published host ports. */
public final class Rest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** What a JobManager answered: the status and the body, or status -1 and the failure. */
    public record Response(int status, String body) {

        public boolean ok() {
            return status == 200;
        }

        public JsonNode json() throws IOException {
            return JSON.readTree(body);
        }

        /** The status and, for an error, the first line of the message and its causes. */
        public String describe() {
            if (status < 0) {
                return "no response: " + body;
            }
            return "HTTP " + status + ": " + errorSummary(body);
        }
    }

    private final HttpClient client =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    /** The parsed body, or empty if the JobManager does not answer with 200. */
    public Optional<JsonNode> get(int hostPort, String path) {
        final Response response =
                exchange(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + hostPort + path))
                                .timeout(Duration.ofSeconds(5))
                                .GET()
                                .build());
        if (!response.ok()) {
            return Optional.empty();
        }
        try {
            return Optional.of(response.json());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Response postJson(int hostPort, String path, String body) {
        return exchange(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + hostPort + path))
                        .timeout(Duration.ofSeconds(60))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build());
    }

    /** Uploads a file as multipart/form-data, like curl -F "jarfile=@x.jar". */
    public Response postFile(int hostPort, String path, String field, Path file)
            throws IOException {
        final String boundary = "----failover" + UUID.randomUUID();
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(
                ("--"
                                + boundary
                                + "\r\nContent-Disposition: form-data; name=\""
                                + field
                                + "\"; filename=\""
                                + file.getFileName()
                                + "\"\r\nContent-Type: application/java-archive\r\n\r\n")
                        .getBytes(StandardCharsets.UTF_8));
        body.write(Files.readAllBytes(file));
        body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        return exchange(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + hostPort + path))
                        .timeout(Duration.ofSeconds(120))
                        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                        .build());
    }

    /**
     * The summary of the message of a Flink error response (see {@link Causes}). A body which is
     * not a Flink error is summarized as it is.
     */
    static String errorSummary(String body) {
        String text = body;
        try {
            final JsonNode errors = JSON.readTree(body).path("errors");
            if (errors.isArray() && !errors.isEmpty()) {
                final List<String> messages = new ArrayList<>();
                errors.forEach(error -> messages.add(error.asText()));
                text = String.join("\n", messages);
            }
        } catch (IOException e) {
            // not JSON, the text is used as it is
        }
        return Causes.summarize(text, 1);
    }

    private Response exchange(HttpRequest request) {
        try {
            final HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        } catch (IOException e) {
            return new Response(-1, e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Response(-1, e.toString());
        }
    }
}
