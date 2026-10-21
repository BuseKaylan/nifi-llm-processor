package io.github.busekaylan.nifi.llm;

/**
 * One request to a model.
 *
 * @param systemPrompt optional role and rules for the model
 * @param prompt       the instruction
 * @param document     optional content the instruction is about, sent before the prompt
 * @param outputSchema optional JSON Schema the answer must match
 * @param effort       one of {@code low, medium, high, xhigh, max} or {@link #EFFORT_NOT_SET}; each provider maps it
 *                     to its closest setting
 * @param maxTokens    upper limit on generated tokens
 */
public record LlmRequest(String systemPrompt, String prompt, String document, String outputSchema, String effort,
                         long maxTokens) {

    public static final String EFFORT_NOT_SET = "not-set";

    public boolean hasEffort() {
        return effort != null && !EFFORT_NOT_SET.equals(effort);
    }

    /** The document in tags followed by the prompt; long content goes before the instruction. */
    public String userText() {
        return document == null ? prompt : wrappedDocument() + "\n\n" + prompt;
    }

    public String wrappedDocument() {
        return "<document>\n" + document + "\n</document>";
    }
}
