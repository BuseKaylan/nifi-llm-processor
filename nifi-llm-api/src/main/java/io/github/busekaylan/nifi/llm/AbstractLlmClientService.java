package io.github.busekaylan.nifi.llm;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Shared configuration and lifecycle for the provider services: API key, model, base URL, timeout and retries.
 *
 * @param <C> the provider SDK's client type
 */
public abstract class AbstractLlmClientService<C> extends AbstractControllerService
        implements LlmClientService, VerifiableControllerService {

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("The provider's API key.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASE_URL = new PropertyDescriptor.Builder()
            .name("Base URL")
            .description("Optional API base URL, for an API gateway, proxy or compatible endpoint.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Request Timeout")
            .description("Maximum time to wait for one API request, per attempt.")
            .required(true)
            .defaultValue("5 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_RETRIES = new PropertyDescriptor.Builder()
            .name("Max Retries")
            .description("Number of automatic retries, with exponential backoff, for rate limits, server errors and network errors.")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.createLongValidator(0, 10, true))
            .build();

    private volatile C client;
    private volatile String model;

    /** The Model property, with a provider-specific default (or none). */
    protected abstract PropertyDescriptor modelProperty();

    /** Provider-specific properties, listed after the common ones. */
    protected List<PropertyDescriptor> providerProperties() {
        return List.of();
    }

    protected abstract C createClient(ConnectionSettings settings, ConfigurationContext context);

    protected abstract void closeClient(C client);

    protected abstract LlmResponse complete(C client, String model, LlmRequest request);

    /** Checks that the API key works and the model exists; throws on failure. */
    protected abstract void verifyModel(C client, String model);

    /** Connection settings read from the common properties. */
    public record ConnectionSettings(String apiKey, String baseUrl, Duration timeout, int maxRetries) {
    }

    @Override
    protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(List.of(API_KEY, modelProperty(), BASE_URL, REQUEST_TIMEOUT, MAX_RETRIES));
        properties.addAll(providerProperties());
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        model = context.getProperty(modelProperty()).getValue().trim();
        client = createClient(settings(context), context);
    }

    @OnDisabled
    public void onDisabled() {
        final C current = client;
        client = null;
        if (current != null) {
            closeClient(current);
        }
    }

    @Override
    public String getModel() {
        return model;
    }

    @Override
    public LlmResponse complete(final LlmRequest request) {
        final C current = client;
        if (current == null) {
            throw new LlmException(getProvider() + " client service is not enabled", null, false, null);
        }
        return complete(current, model, request);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> variables) {
        final String verifyModel = context.getProperty(modelProperty()).getValue().trim();
        C verifyClient = null;
        try {
            verifyClient = createClient(settings(context), context);
            verifyModel(verifyClient, verifyModel);
            return List.of(result(Outcome.SUCCESSFUL, "Authenticated and found model " + verifyModel));
        } catch (final Exception e) {
            verificationLogger.error("{} API verification failed", getProvider(), e);
            return List.of(result(Outcome.FAILED, "Could not retrieve model " + verifyModel + ": " + e.getMessage()));
        } finally {
            if (verifyClient != null) {
                closeClient(verifyClient);
            }
        }
    }

    private ConfigVerificationResult result(final Outcome outcome, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName("Connect to " + getProvider() + " API")
                .outcome(outcome)
                .explanation(explanation)
                .build();
    }

    private static ConnectionSettings settings(final ConfigurationContext context) {
        return new ConnectionSettings(
                context.getProperty(API_KEY).getValue(),
                context.getProperty(BASE_URL).getValue(),
                Duration.ofMillis(context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                context.getProperty(MAX_RETRIES).asInteger());
    }

    protected static PropertyDescriptor.Builder modelPropertyBuilder() {
        return new PropertyDescriptor.Builder()
                .name("Model")
                .required(true)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR);
    }
}
