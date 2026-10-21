package io.github.busekaylan.nifi.llm;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.core.JsonValue;
import com.anthropic.core.http.HttpResponseFor;
import com.anthropic.errors.AnthropicIoException;
import com.anthropic.errors.AnthropicServiceException;
import com.anthropic.models.messages.ContentBlockParam;
import com.anthropic.models.messages.Message;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.OutputConfig;
import com.anthropic.models.messages.TextBlockParam;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Tags({"llm", "ai", "genai", "anthropic", "claude"})
@CapabilityDescription("Connects to Anthropic Claude through the official Anthropic Java SDK (Messages API).")
public class AnthropicLlmClientService extends AbstractLlmClientService<AnthropicClient> {

    static final String FALLBACK_BETA = "server-side-fallback-2026-07-01";
    // Server-side refusal fallback is only accepted for these model families on the Claude API.
    private static final List<String> FALLBACK_MODEL_PREFIXES = List.of(
            "claude-opus-5", "claude-fable-5-1", "claude-sonnet-5-5");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final PropertyDescriptor MODEL = modelPropertyBuilder()
            .description("Claude model id, for example claude-opus-5-5, claude-sonnet-5-5 or claude-haiku-5-5.")
            .defaultValue("claude-opus-5-5")
            .build();

    static final PropertyDescriptor REFUSAL_FALLBACK = new PropertyDescriptor.Builder()
            .name("Refusal Fallback")
            .description("When true, a request declined by safety classifiers is retried server-side on a fallback model "
                    + "within the same call. Applied only to models that support it (Opus 5.x, Fable 5.1, Sonnet 5.5) and "
                    + "only on the Claude API; disable it when using a gateway that does not support it.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private volatile boolean refusalFallback;

    @Override
    public String getProvider() {
        return "anthropic";
    }

    @Override
    protected PropertyDescriptor modelProperty() {
        return MODEL;
    }

    @Override
    protected List<PropertyDescriptor> providerProperties() {
        return List.of(REFUSAL_FALLBACK);
    }

    @Override
    protected AnthropicClient createClient(final ConnectionSettings settings, final ConfigurationContext context) {
        refusalFallback = context.getProperty(REFUSAL_FALLBACK).asBoolean();
        final AnthropicOkHttpClient.Builder builder = AnthropicOkHttpClient.builder()
                .apiKey(settings.apiKey())
                .timeout(settings.timeout())
                .maxRetries(settings.maxRetries());
        if (settings.baseUrl() != null) {
            builder.baseUrl(settings.baseUrl());
        }
        return builder.build();
    }

    @Override
    protected void closeClient(final AnthropicClient client) {
        client.close();
    }

    @Override
    protected void verifyModel(final AnthropicClient client, final String model) {
        client.models().retrieve(model);
    }

    @Override
    protected LlmResponse complete(final AnthropicClient client, final String model, final LlmRequest request) {
        try (HttpResponseFor<Message> response = client.messages().withRawResponse().create(buildParams(model, request))) {
            final String requestId = response.headers().values("request-id").stream().findFirst().orElse(null);
            final Message message = response.parse();
            final String stopReason = message.stopReason().map(reason -> reason.asString()).orElse("");
            final String text = message.content().stream()
                    .flatMap(block -> block.text().stream())
                    .map(textBlock -> textBlock.text())
                    .collect(Collectors.joining());
            final String category = message.stopDetails()
                    .flatMap(details -> details.category())
                    .map(value -> value.asString())
                    .orElse(null);
            return new LlmResponse(text, message.model().asString(), stopReason,
                    message.usage().inputTokens(), message.usage().outputTokens(), requestId,
                    "refusal".equals(stopReason), category, "max_tokens".equals(stopReason));
        } catch (final AnthropicServiceException e) {
            throw new LlmException(e.getMessage(), e.statusCode(), LlmException.isRetryableStatus(e.statusCode()), e);
        } catch (final AnthropicIoException e) {
            throw new LlmException(e.getMessage(), null, true, e);
        }
    }

    MessageCreateParams buildParams(final String model, final LlmRequest request) {
        final MessageCreateParams.Builder builder = MessageCreateParams.builder()
                .model(model)
                .maxTokens(request.maxTokens());
        if (request.systemPrompt() != null) {
            builder.system(request.systemPrompt());
        }
        final List<ContentBlockParam> blocks = new ArrayList<>();
        if (request.document() != null) {
            blocks.add(ContentBlockParam.ofText(TextBlockParam.builder().text(request.wrappedDocument()).build()));
        }
        blocks.add(ContentBlockParam.ofText(TextBlockParam.builder().text(request.prompt()).build()));
        builder.addUserMessageOfBlockParams(blocks);

        final Map<String, Object> schema = schemaMap(request.outputSchema());
        if (request.hasEffort() || schema != null) {
            final OutputConfig.Builder outputConfig = OutputConfig.builder();
            if (request.hasEffort()) {
                outputConfig.effort(OutputConfig.Effort.of(request.effort()));
            }
            if (schema != null) {
                outputConfig.putAdditionalProperty("format", JsonValue.from(Map.of("type", "json_schema", "schema", schema)));
            }
            builder.outputConfig(outputConfig.build());
        }

        if (refusalFallback && FALLBACK_MODEL_PREFIXES.stream().anyMatch(model::startsWith)) {
            builder.putAdditionalHeader("anthropic-beta", FALLBACK_BETA);
            builder.putAdditionalBodyProperty("fallbacks", JsonValue.from("default"));
        }
        return builder.build();
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
