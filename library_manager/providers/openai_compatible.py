"""Generic OpenAI-compatible AI provider.

Works with local servers such as llama.cpp, LM Studio, vLLM, and LocalAI.
The API key is optional because many local servers do not require one.
"""

import logging

import requests

logger = logging.getLogger(__name__)

DEFAULT_OPENAI_COMPATIBLE_URL = "http://localhost:8080/v1"


def normalize_openai_compatible_url(url):
    """Return an API base URL ending in /v1."""
    base_url = (url or DEFAULT_OPENAI_COMPATIBLE_URL).strip().rstrip("/")
    for suffix in ("/chat/completions", "/models"):
        if base_url.endswith(suffix):
            base_url = base_url[:-len(suffix)].rstrip("/")
            break
    if not base_url.endswith("/v1"):
        base_url = f"{base_url}/v1"
    return base_url


def _headers(config):
    headers = {"Content-Type": "application/json"}
    api_key = (config.get("openai_compatible_api_key") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _model_ids(payload):
    """Extract model IDs from common OpenAI-compatible response shapes."""
    if not isinstance(payload, dict):
        return []

    models = payload.get("data")
    if not isinstance(models, list):
        models = payload.get("models", [])
    if not isinstance(models, list):
        return []

    result = []
    for model in models:
        if isinstance(model, str):
            model_id = model
        elif isinstance(model, dict):
            model_id = model.get("id") or model.get("name") or model.get("model")
        else:
            continue
        if isinstance(model_id, str) and model_id.strip() and model_id not in result:
            result.append(model_id.strip())
    return result


def get_openai_compatible_models(config):
    """Fetch model IDs from an OpenAI-compatible /models endpoint."""
    base_url = normalize_openai_compatible_url(config.get("openai_compatible_url"))
    try:
        response = requests.get(
            f"{base_url}/models",
            headers=_headers(config),
            timeout=10,
        )
        if response.status_code == 200:
            return _model_ids(response.json())
    except (requests.RequestException, ValueError) as exc:
        logger.debug("OpenAI-compatible model discovery failed: %s", exc)
    return []


def _resolve_model(config):
    model = (config.get("openai_compatible_model") or "").strip()
    if model:
        return model

    models = get_openai_compatible_models(config)
    if len(models) == 1:
        return models[0]
    if models:
        logger.warning("OpenAI-compatible server returned multiple models; select one in Settings")
    else:
        logger.warning("OpenAI-compatible model is not configured and discovery returned no models")
    return None


def _response_text(payload):
    try:
        content = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return ""

    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") in (None, "text")
        )
    return ""


def call_openai_compatible(
    prompt,
    config,
    parse_json_fn=None,
    explain_error_fn=None,
    report_error_fn=None,
    timeout=120,
):
    """Send a chat completion request to a user-configured compatible API."""
    model = _resolve_model(config)
    if not model:
        return None

    base_url = normalize_openai_compatible_url(config.get("openai_compatible_url"))
    try:
        response = requests.post(
            f"{base_url}/chat/completions",
            headers=_headers(config),
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "stream": False,
            },
            timeout=timeout,
        )
        if response.status_code == 200:
            text = _response_text(response.json())
            if text:
                return parse_json_fn(text) if parse_json_fn else {"raw_response": text}
            logger.warning("OpenAI-compatible server returned no message content")
        else:
            if explain_error_fn:
                message = explain_error_fn(response.status_code, "OpenAI-compatible API")
            else:
                message = f"HTTP {response.status_code}"
            logger.warning("OpenAI-compatible API: %s", message)
    except requests.exceptions.Timeout:
        logger.error("OpenAI-compatible API timed out after %s seconds", timeout)
        if report_error_fn:
            report_error_fn(
                f"OpenAI-compatible API timeout after {timeout} seconds",
                context="openai_compatible_api",
            )
    except requests.exceptions.ConnectionError:
        logger.error("OpenAI-compatible API connection failed at %s", base_url)
        if report_error_fn:
            report_error_fn(
                "OpenAI-compatible API connection failed",
                context="openai_compatible_api",
            )
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        logger.error("OpenAI-compatible API: %s", exc)
        if report_error_fn:
            report_error_fn(
                f"OpenAI-compatible API error: {exc}",
                context="openai_compatible_api",
            )
    return None


def call_openai_compatible_simple(prompt, config, parse_json_fn=None):
    """Make a shorter compatible API request for localization queries."""
    return call_openai_compatible(
        prompt,
        config,
        parse_json_fn=parse_json_fn,
        timeout=60,
    )


def test_openai_compatible_connection(config):
    """Test model discovery without running an inference request."""
    base_url = normalize_openai_compatible_url(config.get("openai_compatible_url"))
    try:
        response = requests.get(
            f"{base_url}/models",
            headers=_headers(config),
            timeout=10,
        )
        if response.status_code == 200:
            models = _model_ids(response.json())
            return {
                "success": True,
                "models": models,
                "model_count": len(models),
            }
        return {"success": False, "error": f"HTTP {response.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": f"Cannot connect to {base_url}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "Connection timed out"}
    except (requests.RequestException, ValueError) as exc:
        return {"success": False, "error": str(exc)}


__all__ = [
    "DEFAULT_OPENAI_COMPATIBLE_URL",
    "normalize_openai_compatible_url",
    "call_openai_compatible",
    "call_openai_compatible_simple",
    "get_openai_compatible_models",
    "test_openai_compatible_connection",
]
