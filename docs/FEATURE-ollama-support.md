# Ollama Self-Hosted AI Support

**Discussion:** #13
**Status:** Implemented

Library Manager supports Ollama as a self-hosted AI provider for book identification, transcript parsing, and text verification.

## Configuration

In **Settings > Engine**, select **Ollama (Self-hosted)** and enter the Ollama server URL. Library Manager requests the server's live model list from `/api/tags`. The model field stays editable for servers or proxies where discovery is unavailable.

No model name is built into Library Manager. If Ollama returns exactly one model, it is selected automatically. If it returns multiple models, select one from the list.

## Docker Networking

The Ollama URL must be reachable from inside the Library Manager container. On Docker Desktop, this is commonly `http://host.docker.internal:11434`. On Linux, expose an appropriate host address or add the host-gateway mapping for your deployment.

## Other Local Servers

For llama.cpp, LM Studio, vLLM, LocalAI, and similar servers, select **llama.cpp / OpenAI-compatible** instead. That provider uses `/v1/models` for discovery and `/v1/chat/completions` for inference, with an optional bearer token.
