package io.github.busekaylan.nifi.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.core.http.HttpResponseFor;
import com.openai.errors.OpenAIIoException;
import com.openai.errors.OpenAIServiceException;
import com.openai.models.ReasoningEffort;
import com.openai.models.ResponseFormatJsonSchema;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessage;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.Map;

@Tags({"llm", "ai", "genai", "openai", "gpt", "chatgpt"})
@CapabilityDescription("Connects to OpenAI GPT models through the official OpenAI Java SDK (Chat Completions API). "
        + "Base URL can point to any OpenAI-compatible endpoint.")
public class OpenAiLlmClientService extends AbstractLlmClientService<OpenAIClient> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final PropertyDescriptor MODEL = modelPropertyBuilder()
            .description("OpenAI model id, for example gpt-5.5.")
            .build();

    @Override
    public String getProvider() {
        return "openai";
    }

    @Override
    protected PropertyDescriptor modelProperty() {
        return MODEL;
    }

    @Override
    protected OpenAIClient createClient(final ConnectionSettings settings, final ConfigurationContext context) {
        final OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder()
                .apiKey(settings.apiKey())
                .timeout(settings.timeout())
                .maxRetries(settings.maxRetries());
        if (settings.baseUrl() != null) {
            builder.baseUrl(settings.baseUrl());
        }
        return builder.build();
    }

    @Override
    protected void closeClient(final OpenAIClient client) {
        client.close();
    }

    @Override
    protected void verifyModel(final OpenAIClient client, final String model) {
        client.models().retrieve(model);
    }

    @Override
    protected LlmResponse complete(final OpenAIClient client, final String model, final LlmRequest request) {
        try (HttpResponseFor<ChatCompletion> response = client.chat().completions().withRawResponse().create(buildParams(model, request))) {
            final String requestId = response.requestId().orElse(null);
            final ChatCompletion completion = response.parse();
            final ChatCompletion.Choice choice = completion.choices().getFirst();
            final ChatCompletionMessage message = choice.message();
            final String finishReason = choice.finishReason().asString();
            final String refusal = message.refusal().orElse(null);
            final long inputTokens = completion.usage().map(usage -> usage.promptTokens()).orElse(0L);
            final long outputTokens = completion.usage().map(usage -> usage.completionTokens()).orElse(0L);
            return new LlmResponse(message.content().orElse(""), completion.model(), finishReason,
                    inputTokens, outputTokens, requestId,
                    refusal != null || "content_filter".equals(finishReason), refusal, "length".equals(finishReason));
        } catch (final OpenAIServiceException e) {
            throw new LlmException(e.getMessage(), e.statusCode(), LlmException.isRetryableStatus(e.statusCode()), e);
        } catch (final OpenAIIoException e) {
            throw new LlmException(e.getMessage(), null, true, e);
        }
    }

    ChatCompletionCreateParams buildParams(final String model, final LlmRequest request) {
        final ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder()
                .model(model)
                .maxCompletionTokens(request.maxTokens());
        if (request.systemPrompt() != null) {
            builder.addSystemMessage(request.systemPrompt());
        }
        builder.addUserMessage(request.userText());

        if (request.hasEffort()) {
            builder.reasoningEffort(ReasoningEffort.of(effort(request.effort())));
        }
        final Map<String, Object> schema = schemaMap(request.outputSchema());
        if (schema != null) {
            final ResponseFormatJsonSchema.JsonSchema.Schema.Builder schemaBuilder = ResponseFormatJsonSchema.JsonSchema.Schema.builder();
            schema.forEach((key, value) -> schemaBuilder.putAdditionalProperty(key, JsonValue.from(value)));
            builder.responseFormat(ResponseFormatJsonSchema.builder()
                    .jsonSchema(ResponseFormatJsonSchema.JsonSchema.builder()
                            .name("output")
                            .strict(true)
                            .schema(schemaBuilder.build())
                            .build())
                    .build());
        }
        return builder.build();
    }

    /** OpenAI's highest reasoning effort is xhigh. */
    static String effort(final String effort) {
        return "max".equals(effort) ? "xhigh" : effort;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> schemaMap(final String schema) {
        if (schema == null) {
            return null;
        }
        try {
            return MAPPER.readValue(schema, Map.class);
        } catch (final Exception e) {
            throw new LlmException("Output JSON Schema is not valid JSON: " + e.getMessage(), null, false, e);
        }
    }
}
