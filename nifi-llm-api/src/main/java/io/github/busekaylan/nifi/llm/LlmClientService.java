package io.github.busekaylan.nifi.llm;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/** A connection to one model of one LLM provider. Implementations must be thread-safe. */
@Tags({"llm", "ai", "genai", "client"})
@CapabilityDescription("Sends prompts to a large language model and returns its answers.")
public interface LlmClientService extends ControllerService {

    /** Short provider id, for example {@code anthropic}, {@code openai} or {@code gemini}. */
    String getProvider();

    /** The model id this service sends requests to. */
    String getModel();

    /**
     * Sends one request to the model.
     *
     * @throws LlmException when the API call fails after the client's own retries
     */
    LlmResponse complete(LlmRequest request);
}
