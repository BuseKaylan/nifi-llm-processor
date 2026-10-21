package io.github.busekaylan.nifi.llm;

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeminiLlmClientServiceTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{\"category\":{\"type\":\"string\"}}}";
    private MockApi api;
    private TestRunner runner;
    private GeminiLlmClientService service;

    @BeforeEach
    void setUp() throws Exception {
        api = new MockApi();
        api.reply(200, response("{\\\"category\\\":\\\"billing\\\"}", "STOP"));
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new GeminiLlmClientService();
        runner.addControllerService("llm", service);
        runner.setProperty(service, AbstractLlmClientService.API_KEY, "test-key");
        runner.setProperty(service, GeminiLlmClientService.MODEL, "gemini-flash-latest");
        runner.setProperty(service, AbstractLlmClientService.BASE_URL, api.url());
        runner.setProperty(service, AbstractLlmClientService.MAX_RETRIES, "0");
        runner.setProperty(service, AbstractLlmClientService.REQUEST_TIMEOUT, "5 sec");
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static String response(final String text, final String finishReason) {
        return """
                {"candidates":[{"content":{"role":"model","parts":[
                    {"text":"thinking about it","thought":true},{"text":"%s"}]},"finishReason":"%s","index":0}],
                 "usageMetadata":{"promptTokenCount":33,"candidatesTokenCount":10,"totalTokenCount":43},
                 "modelVersion":"gemini-flash-latest","responseId":"resp_gemini"}
                """.formatted(text, finishReason);
    }

    private static LlmRequest request(final String schema, final String effort) {
        return new LlmRequest("You are a triage assistant.", "Classify this ticket.", "I was charged twice", schema, effort, 1024);
    }

    @Test
    void requiresModel() {
        runner.removeProperty(service, GeminiLlmClientService.MODEL);
        runner.assertNotValid(service);
    }

    @Test
    void sendsGenerateContentRequestAndParsesAnswer() {
        runner.enableControllerService(service);
        final LlmResponse response = service.complete(request(SCHEMA, "xhigh"));
        assertEquals("{\"category\":\"billing\"}", response.text(), "thought parts are skipped");
        assertEquals("gemini-flash-latest", response.model());
        assertEquals("STOP", response.stopReason());
        assertEquals(33, response.inputTokens());
        assertEquals(10, response.outputTokens());
        assertEquals("resp_gemini", response.requestId());
        assertFalse(response.refused());

        final MockApi.Request sent = api.last();
        assertTrue(sent.path().endsWith("/models/gemini-flash-latest:generateContent"), sent.path());
        assertEquals("test-key", sent.header("x-goog-api-key"));
        assertEquals("<document>\nI was charged twice\n</document>\n\nClassify this ticket.", sent.body().at("/contents/0/parts/0/text").asText());
        assertEquals("You are a triage assistant.", sent.body().at("/systemInstruction/parts/0/text").asText());
        assertEquals("application/json", sent.body().at("/generationConfig/responseMimeType").asText());
        assertEquals("object", sent.body().at("/generationConfig/responseJsonSchema/type").asText());
        assertEquals("HIGH", sent.body().at("/generationConfig/thinkingConfig/thinkingLevel").asText(), "xhigh maps to Gemini's HIGH");
    }

    @Test
    void detectsBlockedAndTruncatedResponses() {
        runner.enableControllerService(service);
        api.reply(200, response("", "SAFETY"));
        final LlmResponse safety = service.complete(request(null, "low"));
        assertTrue(safety.refused());
        assertEquals("SAFETY", safety.refusalReason());

        api.reply(200, """
                {"promptFeedback":{"blockReason":"PROHIBITED_CONTENT"},
                 "usageMetadata":{"promptTokenCount":12,"totalTokenCount":12},"modelVersion":"gemini-flash-latest"}
                """);
        assertEquals("PROHIBITED_CONTENT", service.complete(request(null, "low")).refusalReason());

        api.reply(200, response("{\\\"cat", "MAX_TOKENS"));
        assertTrue(service.complete(request(null, "low")).truncated());
    }

    @Test
    void mapsErrors() {
        runner.enableControllerService(service);
        api.reply(429, "{\"error\":{\"code\":429,\"message\":\"slow down\",\"status\":\"RESOURCE_EXHAUSTED\"}}");
        final LlmException limited = assertThrows(LlmException.class, () -> service.complete(request(null, "low")));
        assertEquals(429, limited.status());
        assertTrue(limited.retryable());
    }
}
