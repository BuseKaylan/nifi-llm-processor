package io.github.busekaylan.nifi.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.genai.Client;
import com.google.genai.errors.ApiException;
import com.google.genai.errors.GenAiIOException;
import com.google.genai.types.Candidate;
import com.google.genai.types.Content;
import com.google.genai.types.FinishReason;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.HttpOptions;
import com.google.genai.types.HttpRetryOptions;
import com.google.genai.types.Part;
import com.google.genai.types.ThinkingConfig;
import com.google.genai.types.ThinkingLevel;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"llm", "ai", "genai", "google", "gemini"})
@CapabilityDescription("Connects to Google Gemini through the official Google Gen AI Java SDK (Gemini Developer API).")
public class GeminiLlmClientService extends AbstractLlmClientService<Client> {

    // Finish reasons that mean the answer was withheld by a safety or policy filter.
    private static final Set<FinishReason.Known> BLOCKED = Set.of(
            FinishReason.Known.SAFETY, FinishReason.Known.PROHIBITED_CONTENT, FinishReason.Known.BLOCKLIST,
            FinishReason.Known.SPII, FinishReason.Known.RECITATION, FinishReason.Known.IMAGE_SAFETY);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final PropertyDescriptor MODEL = modelPropertyBuilder()
            .description("Gemini model id, for example gemini-flash-latest.")
            .build();

    @Override
    public String getProvider() {
        return "gemini";
    }

    @Override
    protected PropertyDescriptor modelProperty() {
        return MODEL;
    }

    @Override
    protected Client createClient(final ConnectionSettings settings, final ConfigurationContext context) {
        final HttpOptions.Builder http = HttpOptions.builder()
                .timeout((int) settings.timeout().toMillis())
                .retryOptions(HttpRetryOptions.builder()
                        .attempts(settings.maxRetries() + 1)
                        .httpStatusCodes(408, 429, 500, 502, 503, 504));
        if (settings.baseUrl() != null) {
            http.baseUrl(settings.baseUrl());
        }
        return Client.builder().apiKey(settings.apiKey()).httpOptions(http.build()).build();
    }

    @Override
    protected void closeClient(final Client client) {
        client.close();
    }

    @Override
    protected void verifyModel(final Client client, final String model) {
        client.models.get(model, null);
    }

    @Override
    protected LlmResponse complete(final Client client, final String model, final LlmRequest request) {
        final GenerateContentResponse response;
        try {
            response = client.models.generateContent(model, request.userText(), buildConfig(request));
        } catch (final ApiException e) {
            throw new LlmException(e.getMessage(), e.code(), LlmException.isRetryableStatus(e.code()), e);
        } catch (final GenAiIOException e) {
            throw new LlmException(e.getMessage(), null, true, e);
        }

        final Optional<String> blockReason = response.promptFeedback()
                .flatMap(feedback -> feedback.blockReason())
                .map(reason -> reason.toString());
        final Optional<FinishReason> finishReason = response.candidates()
                .flatMap(candidates -> candidates.stream().findFirst())
                .flatMap(Candidate::finishReason);
        final Optional<FinishReason> blockedFinish = finishReason.filter(reason -> BLOCKED.contains(reason.knownEnum()));
        final boolean blocked = blockReason.isPresent() || blockedFinish.isPresent();
        final String stopReason = finishReason.map(FinishReason::toString).orElse(blockReason.isPresent() ? "BLOCKED" : "");
        final Optional<GenerateContentResponseUsageMetadata> usage = response.usageMetadata();

        return new LlmResponse(
                blocked ? "" : text(response),
                response.modelVersion().orElse(model),
                stopReason,
                usage.flatMap(GenerateContentResponseUsageMetadata::promptTokenCount).orElse(0),
                usage.flatMap(GenerateContentResponseUsageMetadata::candidatesTokenCount).orElse(0),
                response.responseId().orElse(null),
                blocked,
                blockReason.orElse(blockedFinish.map(FinishReason::toString).orElse(null)),
                finishReason.map(reason -> reason.knownEnum() == FinishReason.Known.MAX_TOKENS).orElse(false));
    }

    /** Joins the text parts of the first candidate, skipping thought summaries. */
    private static String text(final GenerateContentResponse response) {
        return response.candidates()
                .flatMap(candidates -> candidates.stream().findFirst())
                .flatMap(Candidate::content)
                .flatMap(Content::parts)
                .map(parts -> parts.stream()
                        .filter(part -> !part.thought().orElse(false))
                        .map(part -> part.text().orElse(""))
                        .collect(Collectors.joining()))
                .orElse("");
    }

    static GenerateContentConfig buildConfig(final LlmRequest request) {
        final GenerateContentConfig.Builder builder = GenerateContentConfig.builder()
                .maxOutputTokens((int) request.maxTokens());
        if (request.systemPrompt() != null) {
            builder.systemInstruction(Content.fromParts(Part.fromText(request.systemPrompt())));
        }
        if (request.hasEffort()) {
            builder.thinkingConfig(ThinkingConfig.builder().thinkingLevel(thinkingLevel(request.effort())).build());
        }
        final Map<String, Object> schema = schemaMap(request.outputSchema());
        if (schema != null) {
            builder.responseMimeType("application/json").responseJsonSchema(schema);
        }
        return builder.build();
    }

    /** Maps effort to Gemini's thinking levels, whose highest is HIGH. */
    static ThinkingLevel.Known thinkingLevel(final String effort) {
        return List.of("xhigh", "max").contains(effort)
                ? ThinkingLevel.Known.HIGH
                : ThinkingLevel.Known.valueOf(effort.toUpperCase(Locale.ROOT));
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
