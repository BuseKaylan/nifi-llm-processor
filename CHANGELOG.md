# Changelog

## 1.0.0

First release.

- `PromptLLM` processor for Apache NiFi 2.12+ (Java 21)
- `LlmClientService` controller services for Anthropic Claude, OpenAI GPT and Google Gemini, each in its own NAR
- Structured outputs through an Output JSON Schema, for every provider
- `success`, `refused` and `failure` relationships with retry-aware error attributes
- Verify support for every client service
- Ready-to-import example flows for each provider
