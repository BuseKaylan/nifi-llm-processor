package io.github.busekaylan.nifi.llm;

/**
 * The model's answer, normalized across providers.
 *
 * @param stopReason    the provider's own finish reason, for example {@code end_turn}, {@code stop} or {@code STOP}
 * @param refused       the model or its safety system declined to answer
 * @param refusalReason provider-specific detail when refused, if any
 * @param truncated     the answer was cut off by the output token limit
 */
public record LlmResponse(String text, String model, String stopReason, long inputTokens, long outputTokens,
                          String requestId, boolean refused, String refusalReason, boolean truncated) {
}
