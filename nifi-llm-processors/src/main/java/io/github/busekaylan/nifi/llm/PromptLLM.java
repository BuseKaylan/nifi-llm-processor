package io.github.busekaylan.nifi.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"llm", "ai", "genai", "prompt", "claude", "anthropic", "openai", "gpt", "gemini", "classification", "extraction", "summarization"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends the content of a FlowFile, together with a prompt, to a large language model and writes "
        + "the model's answer to the FlowFile content or to an attribute. The model and its credentials come from an "
        + "LLM Client Service: Anthropic Claude, OpenAI GPT or Google Gemini. When an Output JSON Schema is configured, "
        + "the answer is constrained to valid JSON matching that schema, which makes the processor suitable for "
        + "extraction and classification pipelines.")
@UseCase(
        description = "Classify incoming support tickets into categories.",
        configuration = """
                Set "LLM Client Service" to a configured Anthropic, OpenAI or Gemini client service.
                Set "Prompt" to an instruction such as: Classify this ticket as billing, technical, or other.
                Set "Output JSON Schema" to an object schema with a "category" enum property.
                Route "success" to EvaluateJsonPath or a record processor.
                """
)
@UseCase(
        description = "Summarize a document and keep the original content intact.",
        configuration = """
                Set "Prompt" to: Summarize this document in three sentences.
                Set "Output Destination" to "flowfile-attribute" and "Output Attribute Name" to "summary".
                """
)
@WritesAttributes({
        @WritesAttribute(attribute = PromptLLM.ATTR_PROVIDER, description = "The provider that answered: anthropic, openai or gemini"),
        @WritesAttribute(attribute = PromptLLM.ATTR_MODEL, description = "The model that produced the answer"),
        @WritesAttribute(attribute = PromptLLM.ATTR_STOP_REASON, description = "The provider's own finish reason, for example end_turn, stop or STOP"),
        @WritesAttribute(attribute = PromptLLM.ATTR_INPUT_TOKENS, description = "Input tokens billed for the request"),
        @WritesAttribute(attribute = PromptLLM.ATTR_OUTPUT_TOKENS, description = "Output tokens billed for the request"),
        @WritesAttribute(attribute = PromptLLM.ATTR_REQUEST_ID, description = "The provider's request or response id"),
        @WritesAttribute(attribute = PromptLLM.ATTR_REFUSAL_REASON, description = "Provider-specific detail, when the request was declined"),
        @WritesAttribute(attribute = PromptLLM.ATTR_ERROR_MESSAGE, description = "Error message, when the request failed"),
        @WritesAttribute(attribute = PromptLLM.ATTR_ERROR_STATUS, description = "HTTP status code, when the API returned an error"),
        @WritesAttribute(attribute = PromptLLM.ATTR_ERROR_RETRYABLE, description = "true when the failure is transient (rate limit, overload, network)"),
        @WritesAttribute(attribute = "mime.type", description = "text/plain, or application/json when an Output JSON Schema is set (content destination only)")
})
public class PromptLLM extends AbstractProcessor {

    static final String ATTR_PROVIDER = "llm.provider";
    static final String ATTR_MODEL = "llm.model";
    static final String ATTR_STOP_REASON = "llm.stop.reason";
    static final String ATTR_INPUT_TOKENS = "llm.usage.input.tokens";
    static final String ATTR_OUTPUT_TOKENS = "llm.usage.output.tokens";
    static final String ATTR_REQUEST_ID = "llm.request.id";
    static final String ATTR_REFUSAL_REASON = "llm.refusal.reason";
    static final String ATTR_ERROR_MESSAGE = "llm.error.message";
    static final String ATTR_ERROR_STATUS = "llm.error.status";
    static final String ATTR_ERROR_RETRYABLE = "llm.error.retryable";

    static final AllowableValue DESTINATION_CONTENT = new AllowableValue("flowfile-content", "FlowFile Content",
            "Replace the FlowFile content with the model's answer.");
    static final AllowableValue DESTINATION_ATTRIBUTE = new AllowableValue("flowfile-attribute", "FlowFile Attribute",
            "Write the model's answer to an attribute and keep the original content.");

    static final PropertyDescriptor LLM_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("LLM Client Service")
            .description("The model to use, with its provider credentials.")
            .required(true)
            .identifiesControllerService(LlmClientService.class)
            .build();

    static final PropertyDescriptor SYSTEM_PROMPT = new PropertyDescriptor.Builder()
            .name("System Prompt")
            .description("Optional system prompt that sets the model's role and rules.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PROMPT = new PropertyDescriptor.Builder()
            .name("Prompt")
            .description("The instruction sent to the model. Supports Expression Language, so FlowFile attributes can be "
                    + "referenced. When 'Include FlowFile Content' is true, the content is sent before this prompt.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor INCLUDE_CONTENT = new PropertyDescriptor.Builder()
            .name("Include FlowFile Content")
            .description("Whether the FlowFile content is sent to the model as a document, ahead of the prompt.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Character set used to decode the FlowFile content and encode the answer.")
            .required(true)
            .defaultValue(StandardCharsets.UTF_8.name())
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .dependsOn(INCLUDE_CONTENT, "true")
            .build();

    static final PropertyDescriptor MAX_CONTENT_SIZE = new PropertyDescriptor.Builder()
            .name("Max Content Size")
            .description("FlowFiles larger than this are routed to failure without calling the API. Content is never truncated.")
            .required(true)
            .defaultValue("2 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .dependsOn(INCLUDE_CONTENT, "true")
            .build();

    static final PropertyDescriptor OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("Output JSON Schema")
            .description("Optional JSON Schema (a JSON object). When set, the answer is constrained to valid JSON matching "
                    + "the schema. OpenAI uses strict mode, which needs additionalProperties set to false and every "
                    + "property listed in required.")
            .required(false)
            .addValidator(new JsonObjectValidator())
            .build();

    static final PropertyDescriptor MAX_TOKENS = new PropertyDescriptor.Builder()
            .name("Max Tokens")
            .description("Upper limit on tokens the model may generate. Answers cut off by this limit keep the "
                    + "provider's stop reason in llm.stop.reason.")
            .required(true)
            .defaultValue("16000")
            .addValidator(StandardValidators.createLongValidator(1, 128_000, true))
            .build();

    static final PropertyDescriptor EFFORT = new PropertyDescriptor.Builder()
            .name("Effort")
            .description("How much reasoning the model spends. Maps to effort on Claude, reasoning_effort on OpenAI "
                    + "and thinkingLevel on Gemini. Leave it at Not Set for models without reasoning controls.")
            .required(true)
            .allowableValues(
                    new AllowableValue(LlmRequest.EFFORT_NOT_SET, "Not Set", "Use the model's default."),
                    new AllowableValue("low", "Low", "Fast and cheap; good for simple classification."),
                    new AllowableValue("medium", "Medium", "Balanced."),
                    new AllowableValue("high", "High", "Thorough; good for complex extraction."),
                    new AllowableValue("xhigh", "Extra High", "Very thorough. Gemini uses High."),
                    new AllowableValue("max", "Max", "Maximum quality at maximum cost. OpenAI uses Extra High, Gemini uses High."))
            .defaultValue(LlmRequest.EFFORT_NOT_SET)
            .build();

    static final PropertyDescriptor OUTPUT_DESTINATION = new PropertyDescriptor.Builder()
            .name("Output Destination")
            .description("Where the model's answer is written.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT.getValue())
            .build();

    static final PropertyDescriptor OUTPUT_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Output Attribute Name")
            .description("Attribute that receives the model's answer.")
            .required(true)
            .defaultValue("llm.response")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .dependsOn(OUTPUT_DESTINATION, DESTINATION_ATTRIBUTE)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The model answered the request.")
            .build();
    static final Relationship REL_REFUSED = new Relationship.Builder()
            .name("refused")
            .description("The model or its safety system declined the request. The original content is kept.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The request failed, or a structured answer was truncated. See llm.error.* attributes.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            LLM_CLIENT_SERVICE, SYSTEM_PROMPT, PROMPT, INCLUDE_CONTENT, CHARACTER_SET, MAX_CONTENT_SIZE,
            OUTPUT_SCHEMA, MAX_TOKENS, EFFORT, OUTPUT_DESTINATION, OUTPUT_ATTRIBUTE);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_REFUSED, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final LlmClientService llm = context.getProperty(LLM_CLIENT_SERVICE).asControllerService(LlmClientService.class);

        final boolean includeContent = context.getProperty(INCLUDE_CONTENT).asBoolean();
        final Charset charset = includeContent
                ? Charset.forName(context.getProperty(CHARACTER_SET).getValue())
                : StandardCharsets.UTF_8;

        if (includeContent) {
            final long maxSize = context.getProperty(MAX_CONTENT_SIZE).asDataSize(DataUnit.B).longValue();
            if (flowFile.getSize() > maxSize) {
                final String message = "FlowFile size %d bytes exceeds Max Content Size of %d bytes".formatted(flowFile.getSize(), maxSize);
                getLogger().warn("{} for {}", message, flowFile);
                flowFile = session.putAllAttributes(flowFile, Map.of(
                        ATTR_PROVIDER, llm.getProvider(), ATTR_ERROR_MESSAGE, message, ATTR_ERROR_RETRYABLE, "false"));
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        final String schema = context.getProperty(OUTPUT_SCHEMA).getValue();
        final LlmRequest request = new LlmRequest(
                context.getProperty(SYSTEM_PROMPT).evaluateAttributeExpressions(flowFile).getValue(),
                context.getProperty(PROMPT).evaluateAttributeExpressions(flowFile).getValue(),
                includeContent ? readContent(session, flowFile, charset) : null,
                schema == null || schema.isBlank() ? null : schema,
                context.getProperty(EFFORT).getValue(),
                context.getProperty(MAX_TOKENS).asLong());

        final long start = System.nanoTime();
        final LlmResponse response;
        try {
            response = llm.complete(request);
        } catch (final LlmException e) {
            handleError(context, session, flowFile, llm, e.getMessage(), e.status(), e.retryable(), e);
            return;
        } catch (final RuntimeException e) {
            handleError(context, session, flowFile, llm, String.valueOf(e.getMessage()), null, false, e);
            return;
        }
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_PROVIDER, llm.getProvider());
        attributes.put(ATTR_MODEL, response.model());
        attributes.put(ATTR_STOP_REASON, response.stopReason());
        attributes.put(ATTR_INPUT_TOKENS, String.valueOf(response.inputTokens()));
        attributes.put(ATTR_OUTPUT_TOKENS, String.valueOf(response.outputTokens()));
        if (response.requestId() != null) {
            attributes.put(ATTR_REQUEST_ID, response.requestId());
        }

        if (response.refused()) {
            if (response.refusalReason() != null) {
                attributes.put(ATTR_REFUSAL_REASON, response.refusalReason());
            }
            flowFile = session.putAllAttributes(flowFile, attributes);
            getLogger().info("The model declined the request for {}", flowFile);
            session.transfer(flowFile, REL_REFUSED);
            return;
        }

        if (request.outputSchema() != null && response.truncated()) {
            attributes.put(ATTR_ERROR_MESSAGE, "Structured answer was truncated by Max Tokens; increase Max Tokens");
            attributes.put(ATTR_ERROR_RETRYABLE, "false");
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String text = response.text();
        if (DESTINATION_ATTRIBUTE.getValue().equals(context.getProperty(OUTPUT_DESTINATION).getValue())) {
            attributes.put(context.getProperty(OUTPUT_ATTRIBUTE).getValue(), text);
        } else {
            flowFile = session.write(flowFile, out -> out.write(text.getBytes(charset)));
            attributes.put(CoreAttributes.MIME_TYPE.key(), request.outputSchema() != null ? "application/json" : "text/plain");
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().modifyContent(flowFile, llm.getProvider() + " " + response.model() + " answer", millis);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private void handleError(final ProcessContext context, final ProcessSession session, FlowFile flowFile,
                             final LlmClientService llm, final String message, final Integer status,
                             final boolean retryable, final Exception e) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_PROVIDER, llm.getProvider());
        attributes.put(ATTR_ERROR_MESSAGE, message);
        attributes.put(ATTR_ERROR_RETRYABLE, String.valueOf(retryable));
        if (status != null) {
            attributes.put(ATTR_ERROR_STATUS, String.valueOf(status));
        }
        getLogger().error("{} request failed for {} (status={}, retryable={})", llm.getProvider(), flowFile, status, retryable, e);
        flowFile = session.putAllAttributes(flowFile, attributes);
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
        // Back off the whole processor when the API is rate limiting or overloaded.
        if (status != null && (status == 429 || status == 503 || status == 529)) {
            context.yield();
        }
    }

    private static String readContent(final ProcessSession session, final FlowFile flowFile, final Charset charset) {
        final byte[] bytes = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> {
            int offset = 0;
            while (offset < bytes.length) {
                final int read = in.read(bytes, offset, bytes.length - offset);
                if (read < 0) {
                    break;
                }
                offset += read;
            }
        });
        return new String(bytes, charset);
    }

    /** Accepts a JSON object; anything else (arrays, scalars, broken JSON) is rejected. */
    static class JsonObjectValidator implements Validator {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder result = new ValidationResult.Builder().subject(subject).input(input);
            try {
                final JsonNode node = MAPPER.readTree(input);
                if (node == null || !node.isObject()) {
                    return result.valid(false).explanation("must be a JSON object").build();
                }
                return result.valid(true).build();
            } catch (final Exception e) {
                return result.valid(false).explanation("is not valid JSON: " + e.getMessage()).build();
            }
        }
    }
}
