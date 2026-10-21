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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAiLlmClientServiceTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{\"category\":{\"type\":\"string\"}},"
            + "\"required\":[\"category\"],\"additionalProperties\":false}";
    private MockApi api;
    private TestRunner runner;
    private OpenAiLlmClientService service;

    @BeforeEach
    void setUp() throws Exception {
        api = new MockApi();
        api.responseHeaders = Map.of("x-request-id", "req_openai");
        api.reply(200, completion("{\\\"category\\\":\\\"billing\\\"}", "null", "stop"));
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new OpenAiLlmClientService();
        runner.addControllerService("llm", service);
        runner.setProperty(service, AbstractLlmClientService.API_KEY, "test-key");
        runner.setProperty(service, OpenAiLlmClientService.MODEL, "gpt-5.5");
        runner.setProperty(service, AbstractLlmClientService.BASE_URL, api.url());
        runner.setProperty(service, AbstractLlmClientService.MAX_RETRIES, "0");
        runner.setProperty(service, AbstractLlmClientService.REQUEST_TIMEOUT, "5 sec");
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static String completion(final String content, final String refusal, final String finishReason) {
        final String contentJson = content == null ? "null" : "\"" + content + "\"";
        return """
                {"id":"chatcmpl-1","object":"chat.completion","created":1760000000,"model":"gpt-5.5",
                 "choices":[{"index":0,"finish_reason":"%s","logprobs":null,
                   "message":{"role":"assistant","content":%s,"refusal":%s}}],
                 "usage":{"prompt_tokens":31,"completion_tokens":9,"total_tokens":40}}
                """.formatted(finishReason, contentJson, refusal);
    }

    private static LlmRequest request(final String schema, final String effort) {
        return new LlmRequest("You are a triage assistant.", "Classify this ticket.", "I was charged twice", schema, effort, 1024);
    }

    @Test
    void requiresModel() {
        runner.removeProperty(service, OpenAiLlmClientService.MODEL);
        runner.assertNotValid(service);
    }

    @Test
    void sendsChatCompletionRequestAndParsesAnswer() {
        runner.enableControllerService(service);
        final LlmResponse response = service.complete(request(SCHEMA, "max"));
        assertEquals("{\"category\":\"billing\"}", response.text());
        assertEquals("gpt-5.5", response.model());
        assertEquals("stop", response.stopReason());
        assertEquals(31, response.inputTokens());
        assertEquals(9, response.outputTokens());
        assertEquals("req_openai", response.requestId());
        assertFalse(response.refused());

        final MockApi.Request sent = api.last();
        assertEquals("/chat/completions", sent.path());
        assertEquals("Bearer test-key", sent.header("Authorization"));
        assertEquals("gpt-5.5", sent.body().get("model").asText());
        assertEquals(1024, sent.body().get("max_completion_tokens").asInt());
        assertEquals("You are a triage assistant.", sent.body().at("/messages/0/content").asText());
        assertEquals("<document>\nI was charged twice\n</document>\n\nClassify this ticket.", sent.body().at("/messages/1/content").asText());
        assertEquals("xhigh", sent.body().get("reasoning_effort").asText(), "max maps to OpenAI's xhigh");
        assertEquals("json_schema", sent.body().at("/response_format/type").asText());
        assertTrue(sent.body().at("/response_format/json_schema/strict").asBoolean());
    }

    @Test
    void omitsEffortAndSchemaWhenNotSet() {
        runner.enableControllerService(service);
        service.complete(request(null, LlmRequest.EFFORT_NOT_SET));
        assertFalse(api.last().body().has("reasoning_effort"));
        assertFalse(api.last().body().has("response_format"));
    }

    @Test
    void detectsRefusalAndTruncation() {
        runner.enableControllerService(service);
        api.reply(200, completion(null, "\"I can't help with that.\"", "stop"));
        final LlmResponse refused = service.complete(request(SCHEMA, "low"));
        assertTrue(refused.refused());
        assertEquals("I can't help with that.", refused.refusalReason());

        api.reply(200, completion("{\\\"cat", "null", "length"));
        assertTrue(service.complete(request(SCHEMA, "low")).truncated());
    }

    @Test
    void mapsErrors() {
        runner.enableControllerService(service);
        api.reply(429, "{\"error\":{\"message\":\"slow down\",\"type\":\"rate_limit_exceeded\",\"code\":null}}");
        final LlmException limited = assertThrows(LlmException.class, () -> service.complete(request(null, "low")));
        assertEquals(429, limited.status());
        assertTrue(limited.retryable());
    }
}
