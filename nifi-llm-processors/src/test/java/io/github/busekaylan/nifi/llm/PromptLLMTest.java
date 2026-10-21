package io.github.busekaylan.nifi.llm;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PromptLLMTest {

    /** Records requests and answers with whatever the test configures. */
    static class FakeLlmClientService extends AbstractControllerService implements LlmClientService {
        final List<LlmRequest> requests = new ArrayList<>();
        Function<LlmRequest, LlmResponse> answer = request -> response("Short summary.", "stop");

        @Override
        public String getProvider() {
            return "openai";
        }

        @Override
        public String getModel() {
            return "gpt-5.5";
        }

        @Override
        public LlmResponse complete(final LlmRequest request) {
            requests.add(request);
            return answer.apply(request);
        }
    }

    private TestRunner runner;
    private FakeLlmClientService llm;

    static LlmResponse response(final String text, final String stopReason) {
        return new LlmResponse(text, "gpt-5.5", stopReason, 42, 7, "req_1", false, null, false);
    }

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(PromptLLM.class);
        llm = new FakeLlmClientService();
        runner.addControllerService("llm", llm);
        runner.enableControllerService(llm);
        runner.setProperty(PromptLLM.LLM_CLIENT_SERVICE, "llm");
        runner.setProperty(PromptLLM.PROMPT, "Summarize for ${team}.");
    }

    @Test
    void writesAnswerToContent() {
        runner.setProperty(PromptLLM.SYSTEM_PROMPT, "You are concise.");
        runner.setProperty(PromptLLM.EFFORT, "low");
        runner.enqueue("A long report.", Map.of("team", "finance"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PromptLLM.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_SUCCESS).getFirst();
        out.assertContentEquals("Short summary.");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertAttributeEquals(PromptLLM.ATTR_PROVIDER, "openai");
        out.assertAttributeEquals(PromptLLM.ATTR_MODEL, "gpt-5.5");
        out.assertAttributeEquals(PromptLLM.ATTR_STOP_REASON, "stop");
        out.assertAttributeEquals(PromptLLM.ATTR_INPUT_TOKENS, "42");
        out.assertAttributeEquals(PromptLLM.ATTR_OUTPUT_TOKENS, "7");
        out.assertAttributeEquals(PromptLLM.ATTR_REQUEST_ID, "req_1");

        final LlmRequest request = llm.requests.getFirst();
        assertEquals("You are concise.", request.systemPrompt());
        assertEquals("Summarize for finance.", request.prompt());
        assertEquals("A long report.", request.document());
        assertEquals("low", request.effort());
        assertEquals(16_000, request.maxTokens());
        assertNull(request.outputSchema());
    }

    @Test
    void writesAnswerToAttributeAndKeepsContent() {
        runner.setProperty(PromptLLM.OUTPUT_DESTINATION, PromptLLM.DESTINATION_ATTRIBUTE);
        runner.setProperty(PromptLLM.OUTPUT_ATTRIBUTE, "summary");
        runner.enqueue("original", Map.of("team", "ops"));
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_SUCCESS).getFirst();
        out.assertContentEquals("original");
        out.assertAttributeEquals("summary", "Short summary.");
        out.assertAttributeNotExists("mime.type");
    }

    @Test
    void passesSchemaAndMarksJson() {
        runner.setProperty(PromptLLM.OUTPUT_SCHEMA, "{\"type\":\"object\"}");
        llm.answer = request -> response("{\"category\":\"billing\"}", "stop");
        runner.enqueue("My invoice is wrong", Map.of("team", "support"));
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_SUCCESS).getFirst();
        out.assertContentEquals("{\"category\":\"billing\"}");
        out.assertAttributeEquals("mime.type", "application/json");
        assertEquals("{\"type\":\"object\"}", llm.requests.getFirst().outputSchema());
    }

    @Test
    void truncatedStructuredAnswerGoesToFailure() {
        runner.setProperty(PromptLLM.OUTPUT_SCHEMA, "{\"type\":\"object\"}");
        llm.answer = request -> new LlmResponse("{\"cat", "gpt-5.5", "length", 5, 1024, "req_2", false, null, true);
        runner.enqueue("x");
        runner.run();

        runner.assertAllFlowFilesTransferred(PromptLLM.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_FAILURE).getFirst();
        out.assertContentEquals("x");
        out.assertAttributeEquals(PromptLLM.ATTR_STOP_REASON, "length");
    }

    @Test
    void refusalGoesToRefused() {
        llm.answer = request -> new LlmResponse("", "gpt-5.5", "stop", 5, 0, "req_3", true, "I can't help with that.", false);
        runner.enqueue("content");
        runner.run();

        runner.assertAllFlowFilesTransferred(PromptLLM.REL_REFUSED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_REFUSED).getFirst();
        out.assertContentEquals("content");
        out.assertAttributeEquals(PromptLLM.ATTR_REFUSAL_REASON, "I can't help with that.");
    }

    @Test
    void apiErrorGoesToFailure() {
        llm.answer = request -> {
            throw new LlmException("bad model", 400, false, null);
        };
        runner.enqueue("content");
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_FAILURE).getFirst();
        out.assertAttributeEquals(PromptLLM.ATTR_PROVIDER, "openai");
        out.assertAttributeEquals(PromptLLM.ATTR_ERROR_STATUS, "400");
        out.assertAttributeEquals(PromptLLM.ATTR_ERROR_RETRYABLE, "false");
        out.assertAttributeEquals(PromptLLM.ATTR_ERROR_MESSAGE, "bad model");
        assertTrue(out.isPenalized());
    }

    @Test
    void rateLimitIsMarkedRetryable() {
        llm.answer = request -> {
            throw new LlmException("slow down", 429, true, null);
        };
        runner.enqueue("content");
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(PromptLLM.REL_FAILURE).getFirst();
        out.assertAttributeEquals(PromptLLM.ATTR_ERROR_STATUS, "429");
        out.assertAttributeEquals(PromptLLM.ATTR_ERROR_RETRYABLE, "true");
    }

    @Test
    void oversizedContentFailsWithoutCallingModel() {
        runner.setProperty(PromptLLM.MAX_CONTENT_SIZE, "10 B");
        runner.enqueue("this content is longer than ten bytes");
        runner.run();

        runner.assertAllFlowFilesTransferred(PromptLLM.REL_FAILURE, 1);
        assertTrue(llm.requests.isEmpty());
    }

    @Test
    void promptOnlyModeSkipsContent() {
        runner.setProperty(PromptLLM.INCLUDE_CONTENT, "false");
        runner.enqueue("ignored", Map.of("team", "hr"));
        runner.run();

        final LlmRequest request = llm.requests.getFirst();
        assertNull(request.document());
        assertEquals("Summarize for hr.", request.userText());
        assertEquals(LlmRequest.EFFORT_NOT_SET, request.effort());
    }

    @Test
    void invalidSchemaMakesProcessorInvalid() {
        runner.setProperty(PromptLLM.OUTPUT_SCHEMA, "[1,2]");
        runner.assertNotValid();
        runner.setProperty(PromptLLM.OUTPUT_SCHEMA, "{not json");
        runner.assertNotValid();
        runner.setProperty(PromptLLM.OUTPUT_SCHEMA, "{\"type\":\"object\"}");
        runner.assertValid();
    }
}
