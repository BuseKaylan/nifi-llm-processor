package io.github.busekaylan.nifi.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** A local HTTP server that records requests and replies with a canned JSON response. */
final class MockApi implements AutoCloseable {

    record Request(String path, Map<String, List<String>> headers, JsonNode body) {
        String header(final String name) {
            return headers.entrySet().stream()
                    .filter(e -> e.getKey().equalsIgnoreCase(name))
                    .map(e -> String.join(",", e.getValue()))
                    .findFirst().orElse(null);
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpServer server;
    final List<Request> requests = new CopyOnWriteArrayList<>();
    volatile int status = 200;
    volatile String body = "{}";
    volatile Map<String, String> responseHeaders = Map.of();

    MockApi() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            final byte[] in = exchange.getRequestBody().readAllBytes();
            requests.add(new Request(exchange.getRequestURI().getPath(), exchange.getRequestHeaders(),
                    in.length == 0 ? null : MAPPER.readTree(in)));
            final byte[] out = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            responseHeaders.forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            exchange.sendResponseHeaders(status, out.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
    }

    void reply(final int status, final String body) {
        this.status = status;
        this.body = body;
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    Request last() {
        return requests.getLast();
    }

    void stop() {
        server.stop(0);
    }

    @Override
    public void close() {
        stop();
    }
}
