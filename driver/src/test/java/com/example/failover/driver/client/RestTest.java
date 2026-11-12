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

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class RestTest {

    /** What the REST endpoint of a JobManager answers when the user code of a job throws. */
    private static final String FLINK_ERROR =
            "{\"errors\":[\"org.apache.flink.runtime.rest.handler.RestHandlerException: Could not"
                    + " execute application.\\n"
                    + "\\tat org.apache.flink.runtime.webmonitor.handlers.JarRunHandler.lambda"
                    + "$handleRequest$1(JarRunHandler.java:114)\\n"
                    + "Caused by: java.util.concurrent.CompletionException: "
                    + "org.apache.flink.util.FlinkRuntimeException: Could not execute"
                    + " application.\\n"
                    + "\\tat java.base/java.util.concurrent.CompletableFuture.encodeThrowable"
                    + "(CompletableFuture.java:314)\\n"
                    + "Caused by: java.lang.IllegalStateException: boom\\n"
                    + "\\tat com.example.Job.main(Job.java:7)\\n"
                    + "\\t... 3 more\"]}";

    private static final String FLINK_ERROR_SUMMARY =
            "org.apache.flink.runtime.rest.handler.RestHandlerException: Could not execute"
                    + " application. | Caused by: java.util.concurrent.CompletionException:"
                    + " org.apache.flink.util.FlinkRuntimeException: Could not execute"
                    + " application. | Caused by: java.lang.IllegalStateException: boom";

    private record Request(String method, String contentType, byte[] body) {}

    @TempDir Path tempDir;

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Rest rest = new Rest();
    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private int port() {
        return server.getAddress().getPort();
    }

    private void respond(String path, int status, String body) {
        server.createContext(
                path,
                exchange -> {
                    requests.add(
                            new Request(
                                    exchange.getRequestMethod(),
                                    exchange.getRequestHeaders().getFirst("Content-Type"),
                                    exchange.getRequestBody().readAllBytes()));
                    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, bytes.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                });
    }

    /** The port of a server which has been stopped, so that nothing listens there. */
    private int closedPort() {
        final int port = port();
        server.stop(0);
        return port;
    }

    // ------------------------------------------------------------------------------------------
    // get
    // ------------------------------------------------------------------------------------------

    @Test
    void getReturnsTheParsedBodyOfA200() {
        respond("/jobs/overview", 200, "{\"jobs\":[{\"jid\":\"a\",\"state\":\"RUNNING\"}]}");

        final Optional<JsonNode> body = rest.get(port(), "/jobs/overview");

        assertThat(body).isPresent();
        assertThat(body.get().toString())
                .isEqualTo("{\"jobs\":[{\"jid\":\"a\",\"state\":\"RUNNING\"}]}");
        assertThat(requests).extracting(Request::method).containsExactly("GET");
    }

    @Test
    void getIsEmptyForAStatusOtherThan200() {
        respond("/jobs/overview", 503, "{\"errors\":[\"Service temporarily unavailable\"]}");

        assertThat(rest.get(port(), "/jobs/overview")).isEmpty();
        assertThat(requests).hasSize(1);
    }

    @Test
    void getIsEmptyForABodyWhichIsNotJson() {
        respond("/jobs/overview", 200, "<html>not json</html>");

        assertThat(rest.get(port(), "/jobs/overview")).isEmpty();
        assertThat(requests).hasSize(1);
    }

    @Test
    void getIsEmptyIfNothingListens() {
        assertThat(rest.get(closedPort(), "/jobs/overview")).isEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // post
    // ------------------------------------------------------------------------------------------

    @Test
    void postJsonSendsTheBodyAsJsonAndReturnsTheAnswer() throws IOException {
        respond("/jars/x.jar/run", 200, "{\"jobid\":\"0123\"}");

        final Rest.Response response =
                rest.postJson(port(), "/jars/x.jar/run", "{\"parallelism\":1}");

        assertThat(response.ok()).isTrue();
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.json().path("jobid").asText()).isEqualTo("0123");
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).method()).isEqualTo("POST");
        assertThat(requests.get(0).contentType()).isEqualTo("application/json");
        assertThat(new String(requests.get(0).body(), StandardCharsets.UTF_8))
                .isEqualTo("{\"parallelism\":1}");
    }

    @Test
    void postJsonKeepsTheStatusAndTheMessageOfAnError() {
        respond("/jars/x.jar/run", 500, FLINK_ERROR);

        final Rest.Response response =
                rest.postJson(port(), "/jars/x.jar/run", "{\"parallelism\":1}");

        assertThat(response.ok()).isFalse();
        assertThat(response.status()).isEqualTo(500);
        assertThat(response.describe()).isEqualTo("HTTP 500: " + FLINK_ERROR_SUMMARY);
    }

    @Test
    void postJsonReportsTheFailureIfNothingListens() {
        final Rest.Response response =
                rest.postJson(closedPort(), "/jars/x.jar/run", "{\"parallelism\":1}");

        assertThat(response.ok()).isFalse();
        assertThat(response.status()).isEqualTo(-1);
        assertThat(response.describe()).startsWith("no response: java.net.ConnectException");
    }

    @Test
    void postFileSendsTheFileAsAMultipartBodyUnderTheGivenField() throws IOException {
        final Path jar = tempDir.resolve("example.jar");
        Files.write(jar, new byte[] {'P', 'K', 3, 4, 0, (byte) 0xFF, 'z'});
        respond(
                "/jars/upload",
                200,
                "{\"filename\":\"/tmp/flink-web-upload/4e12_example.jar\",\"status\":\"success\"}");

        final Rest.Response response = rest.postFile(port(), "/jars/upload", "jarfile", jar);

        assertThat(response.ok()).isTrue();
        assertThat(response.json().path("filename").asText())
                .isEqualTo("/tmp/flink-web-upload/4e12_example.jar");
        assertThat(requests).hasSize(1);
        final Request request = requests.get(0);
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.contentType()).startsWith("multipart/form-data; boundary=");
        final String boundary =
                request.contentType().substring("multipart/form-data; boundary=".length());
        final byte[] expected =
                concatenate(
                        ("--"
                                        + boundary
                                        + "\r\nContent-Disposition: form-data; name=\"jarfile\";"
                                        + " filename=\"example.jar\"\r\n"
                                        + "Content-Type: application/java-archive\r\n\r\n")
                                .getBytes(StandardCharsets.UTF_8),
                        Files.readAllBytes(jar),
                        ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        assertThat(request.body()).isEqualTo(expected);
    }

    // ------------------------------------------------------------------------------------------
    // error summary
    // ------------------------------------------------------------------------------------------

    @Test
    void errorSummaryOfAFlinkErrorIsTheFirstLineAndTheCauses() {
        assertThat(Rest.errorSummary(FLINK_ERROR)).isEqualTo(FLINK_ERROR_SUMMARY);
    }

    @Test
    void errorSummaryOfAPlainTextBodyIsItsFirstLine() {
        assertThat(Rest.errorSummary("Not Found\nsecond line\nCaused by: x"))
                .isEqualTo("Not Found | Caused by: x");
    }

    @Test
    void errorSummaryOfAnEmptyBodyIsEmpty() {
        assertThat(Rest.errorSummary("")).isEmpty();
    }

    private static byte[] concatenate(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        final byte[] all = new byte[length];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, all, offset, part.length);
            offset += part.length;
        }
        return all;
    }
}
