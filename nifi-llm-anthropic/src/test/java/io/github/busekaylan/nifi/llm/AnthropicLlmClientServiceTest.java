package io.github.busekaylan.nifi.llm;

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnthropicLlmClientServiceTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{\"category\":{\"type\":\"string\"}}}";
    private MockApi api;
    private TestRunner runner;
    private AnthropicLlmClientService service;

    @BeforeEach
    void setUp() throws Exception {
        api = new MockApi();
        api.responseHeaders = Map.of("request-id", "req_anthropic");
        api.reply(200, """
                {"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5",
                 "content":[{"type":"text","text":"{\\"category\\":\\"billing\\"}"}],
                 "stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":30,"output_tokens":8}}
                """);
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new AnthropicLlmClientService();
        runner.addControllerService("llm", service);
        runner.setProperty(service, AbstractLlmClientService.API_KEY, "test-key");
        runner.setProperty(service, AbstractLlmClientService.BASE_URL, api.url());
        runner.setProperty(service, AbstractLlmClientService.MAX_RETRIES, "0");
        runner.setProperty(service, AbstractLlmClientService.REQUEST_TIMEOUT, "5 sec");
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static LlmRequest request(final String schema, final String effort) {
        return new LlmRequest("You are a triage assistant.", "Classify this ticket.", "I was charged twice", schema, effort, 1024);
    }

    @Test
    void defaultsToClaudeOpus() {
        runner.enableControllerService(service);
        assertEquals("anthropic", service.getProvider());
        assertEquals("claude-opus-5-5", service.getModel());
    }

    @Test
    void sendsMessagesRequestAndParsesAnswer() {
        runner.enableControllerService(service);
        final LlmResponse response = service.complete(request(SCHEMA, "low"));
        assertEquals("{\"category\":\"billing\"}", response.text());
        assertEquals("claude-opus-5-5", response.model());
        assertEquals("end_turn", response.stopReason());
        assertEquals(30, response.inputTokens());
        assertEquals(8, response.outputTokens());
        assertEquals("req_anthropic", response.requestId());
        assertFalse(response.refused());
        assertFalse(response.truncated());

        final MockApi.Request sent = api.last();
        assertEquals("/v1/messages", sent.path());
        assertEquals("test-key", sent.header("x-api-key"));
        assertEquals("You are a triage assistant.", sent.body().get("system").asText());
        assertEquals("<document>\nI was charged twice\n</document>", sent.body().at("/messages/0/content/0/text").asText());
        assertEquals("Classify this ticket.", sent.body().at("/messages/0/content/1/text").asText());
        assertEquals("low", sent.body().at("/output_config/effort").asText());
        assertEquals("json_schema", sent.body().at("/output_config/format/type").asText());
        assertEquals("default", sent.body().get("fallbacks").asText());
        assertEquals(AnthropicLlmClientService.FALLBACK_BETA, sent.header("anthropic-beta"));
    }

    @Test
    void omitsUnsupportedOptions() {
        runner.setProperty(service, AnthropicLlmClientService.MODEL, "claude-haiku-5-5");
        runner.enableControllerService(service);
        service.complete(new LlmRequest(null, "Say hi.", null, null, LlmRequest.EFFORT_NOT_SET, 100));
        final MockApi.Request sent = api.last();
        assertFalse(sent.body().has("fallbacks"));
        assertFalse(sent.body().has("output_config"));
        assertFalse(sent.body().has("system"));
        assertEquals(1, sent.body().at("/messages/0/content").size(), "no document block without content");
        assertNull(sent.header("anthropic-beta"));
    }

    @Test
    void detectsRefusal() {
        runner.enableControllerService(service);
        api.reply(200, """
                {"id":"msg_2","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],
                 "stop_reason":"refusal","stop_sequence":null,
                 "stop_details":{"type":"refusal","category":"cyber","explanation":"declined"},
                 "usage":{"input_tokens":5,"output_tokens":0}}
                """);
        final LlmResponse response = service.complete(request(null, "low"));
        assertTrue(response.refused());
        assertEquals("cyber", response.refusalReason());
    }

    @Test
    void mapsErrors() {
        runner.enableControllerService(service);
        api.reply(429, "{\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}");
        final LlmException limited = assertThrows(LlmException.class, () -> service.complete(request(null, "low")));
        assertEquals(429, limited.status());
        assertTrue(limited.retryable());

        api.reply(400, "{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad model\"}}");
        final LlmException bad = assertThrows(LlmException.class, () -> service.complete(request(null, "low")));
        assertEquals(400, bad.status());
        assertFalse(bad.retryable());
    }
}
