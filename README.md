# NiFi LLM Processor

[![CI](https://github.com/BuseKaylan/nifi-llm-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/BuseKaylan/nifi-llm-processor/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![NiFi](https://img.shields.io/badge/Apache%20NiFi-2.12-728e9b)
![Java](https://img.shields.io/badge/Java-21-orange)

[Türkçe](README.tr.md)

An [Apache NiFi](https://nifi.apache.org) 2.x processor for large language models: **`PromptLLM`**, with client services for **Anthropic Claude**, **OpenAI GPT** and **Google Gemini**.

It sends a FlowFile's content together with a prompt to the model, then writes the answer to the FlowFile content or to an attribute. Use it in data pipelines to classify, extract, summarize, translate or normalize data.

![Support tickets classified by an LLM and routed by category in NiFi](docs/images/example-flow.png)

*The [example flow](#try-the-example-flows): the model classifies each support ticket and returns structured JSON, and NiFi routes the ticket by category and urgency.*

> This is an independent community project. It is not affiliated with, endorsed by, or sponsored by Anthropic, OpenAI, Google or the Apache Software Foundation.

Looking for Kafka? See [kafka-llm-processor](https://github.com/BuseKaylan/kafka-llm-processor).

## How it is organized

```
PromptLLM processor  ──uses──▶  LLM Client Service
                                  ├── AnthropicLlmClientService   (Claude)
                                  ├── OpenAiLlmClientService      (GPT)
                                  └── GeminiLlmClientService      (Gemini)
```

- **The processor** holds the task: the prompt, the JSON Schema, the effort and where the answer goes.
- **The client service** holds the connection: the API key, the model, the base URL, timeouts and retries. Many processors can share one service, and you switch providers by pointing the processor at a different service.
- **Each provider is its own NAR.** Install only the providers you use. Each one bundles that provider's official Java SDK.

![The same flow running on Claude, GPT and Gemini](docs/images/three-providers.png)

## Features

- **Structured outputs on every provider.** Set an `Output JSON Schema` and the answer is valid JSON that matches it, ready for `EvaluateJsonPath` or record processors.
- **Expression Language** in `Prompt` and `System Prompt`, so prompts can use FlowFile attributes such as `${filename}`.
- **Three relationships:** `success`, `refused` (the model declined the request) and `failure`.
- **Retries.** Rate limits, server errors and network errors are retried with exponential backoff. If retries run out, the FlowFile goes to `failure` with `llm.error.retryable=true`. On rate limits the processor also yields, and it works with NiFi 2's per-relationship *Retry* settings.
- **Verify support.** The *Verify* button on each client service checks the API key and model with a live call.
- **Size limit.** FlowFiles larger than `Max Content Size` go to `failure` without calling the API. Content is never truncated silently.
- **Provider, model, token usage and request id** are written as attributes on every FlowFile.

## Installation

Download the NARs from the [Releases](https://github.com/BuseKaylan/nifi-llm-processor/releases) page, or [build them yourself](#building). You always need two, plus one per provider you use:

- `nifi-llm-api-nar` (required)
- `nifi-llm-processors-nar` (required)
- `nifi-llm-anthropic-nar` for Claude
- `nifi-llm-openai-nar` for GPT
- `nifi-llm-gemini-nar` for Gemini

Copy them into NiFi's `extensions` directory:

```bash
cp nifi-llm-*-nar-1.0.0.nar $NIFI_HOME/extensions/
```

NiFi 2 loads new NARs from `extensions/` without a restart. When you upgrade to NARs with a new version number, restart NiFi.

**Compatibility:** Apache NiFi 2.12+ and Java 21.

## Try the example flows

The [`examples`](examples) folder has a ready-to-import flow for each provider: `support-ticket-classification-claude.json`, `-gpt.json` and `-gemini.json`. Each one splits nine sample support tickets, has the model classify each one as `billing`, `technical` or `other` and flag urgent ones, then routes each ticket by its category.

1. Install the NARs, including the one for your provider.
2. Drag the **Process Group** icon from the top toolbar onto the canvas, click the upload icon in the dialog, and choose the JSON file.
3. Open the menu (☰) → **Parameter Contexts** → **LLM Example (…)**, and set `llm.api.key` to your API key. Change `llm.model` if you want a different model.
4. Right-click the process group → **Enable All Controller Services**, then right-click it again → **Start**.

Within a few seconds the tickets appear in the `urgent`, `billing`, `technical` and `unmatched` queues. List a queue to see the `ticket.category` and `ticket.urgent` attributes. The flow runs once a day by default; to send the tickets again, right-click **Sample Support Tickets** → **Run Once**.

## PromptLLM properties

![PromptLLM configuration in the NiFi UI](docs/images/processor-properties.png)

- **LLM Client Service:** the client service to use.
- **System Prompt:** optional role and rules for the model. Supports EL.
- **Prompt:** the instruction. Supports EL.
- **Include FlowFile Content:** default `true`. Sends the content in `<document>` tags before the prompt.
- **Character Set:** default `UTF-8`.
- **Max Content Size:** default `2 MB`. Larger FlowFiles go to `failure` without an API call.
- **Output JSON Schema:** optional JSON Schema for structured output. OpenAI uses strict mode, which needs `"additionalProperties": false` and every property listed in `required`.
- **Max Tokens:** default `16000`.
- **Effort:** `Not Set` (default, the model's own default), `Low`, `Medium`, `High`, `Extra High` or `Max`. It maps to `effort` on Claude, `reasoning_effort` on OpenAI (`Max` becomes `xhigh`) and `thinkingLevel` on Gemini (`Extra High` and `Max` become `HIGH`). Leave it at `Not Set` for models without reasoning controls.
- **Output Destination:** `flowfile-content` (default) or `flowfile-attribute`.
- **Output Attribute Name:** default `llm.response`.

## Client service properties

![Anthropic client service configuration](docs/images/service-properties.png)

Every client service has:

- **API Key:** the provider's API key (sensitive).
- **Model:** `claude-opus-5-5` by default on the Anthropic service. Required on the OpenAI and Gemini services, for example `gpt-5.5` or `gemini-flash-latest`.
- **Base URL:** optional API gateway, proxy or compatible endpoint.
- **Request Timeout:** default `5 min`, per attempt.
- **Max Retries:** default `2`.

The Anthropic service also has **Refusal Fallback** (default `true`): on Claude models that support it (Opus 5.x, Fable 5.1, Sonnet 5.5), a declined request is rerouted server-side to a fallback model in the same call. Disable it when using a gateway that doesn't support it.

## Attributes written

- `llm.provider`, `llm.model`
- `llm.stop.reason`: the provider's own value, for example `end_turn` (Claude), `stop` (OpenAI) or `STOP` (Gemini)
- `llm.usage.input.tokens`, `llm.usage.output.tokens`
- `llm.request.id`, useful for support requests
- `llm.refusal.reason`, when the request was declined and the provider gives a reason
- `llm.error.message`, `llm.error.status`, `llm.error.retryable`, on failure
- `mime.type`: `text/plain`, or `application/json` with an Output JSON Schema

### What counts as refused

- **Claude:** stop reason `refusal`.
- **OpenAI:** the message has a `refusal`, or the finish reason is `content_filter`.
- **Gemini:** the prompt was blocked, or the finish reason is a safety or policy reason such as `SAFETY`, `PROHIBITED_CONTENT` or `RECITATION`.

## Notes

- If a structured answer is cut off by `Max Tokens`, the FlowFile goes to `failure` rather than carrying half-finished JSON downstream.
- The processor makes one API call per FlowFile. Raise *Concurrent Tasks* for parallelism; the client services are thread-safe.
- For high-volume, simple tasks, a small model and `Effort` set to `Low` lower cost significantly.

## Building

Requires Java 21 and Maven 3.9+.

```bash
mvn clean install
```

The NARs are written to each `*-nar/target/` folder. The tests use local mock HTTP servers that speak each provider's API, so they need no API keys and cost nothing.

## Project layout

```
nifi-llm-api/          LlmClientService interface and shared types
nifi-llm-processors/   PromptLLM processor
nifi-llm-anthropic/    Anthropic Claude client service
nifi-llm-openai/       OpenAI GPT client service
nifi-llm-gemini/       Google Gemini client service
*-nar/                 The NAR packages NiFi loads
examples/              Ready-to-import NiFi flow definitions
docs/images/           Screenshots and the social preview image
```

## Contributing

Issues and pull requests are welcome. Please run `mvn clean install` before opening a pull request.

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for trademark and third-party notices.
