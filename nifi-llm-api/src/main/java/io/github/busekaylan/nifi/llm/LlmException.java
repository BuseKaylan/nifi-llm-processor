package io.github.busekaylan.nifi.llm;

/** A failed API call. {@code status} is null for network errors. */
public class LlmException extends RuntimeException {

    private final Integer status;
    private final boolean retryable;

    public LlmException(final String message, final Integer status, final boolean retryable, final Throwable cause) {
        super(message, cause);
        this.status = status;
        this.retryable = retryable;
    }

    public Integer status() {
        return status;
    }

    public boolean retryable() {
        return retryable;
    }

    public static boolean isRetryableStatus(final int status) {
        return status == 408 || status == 409 || status == 429 || status >= 500;
    }
}
