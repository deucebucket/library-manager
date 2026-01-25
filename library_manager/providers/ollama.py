"""Ollama AI provider for Library Manager.

This module provides integration with local Ollama servers for fully self-hosted
AI book identification. Ollama runs LLMs locally without any API keys or cloud
dependency.

Functions:
- call_ollama: Main AI call for book identification
- call_ollama_simple: Lightweight call for localization queries
- get_ollama_models: List available models on the server
- test_ollama_connection: Test server connectivity
"""

import logging
import requests

logger = logging.getLogger(__name__)

# Default Ollama settings
DEFAULT_OLLAMA_URL = 'http://localhost:11434'
DEFAULT_OLLAMA_MODEL = 'llama3.2:3b'


def call_ollama(prompt, config, parse_json_fn=None, explain_error_fn=None, report_error_fn=None):
    """
    Call local Ollama API for fully self-hosted AI.

    Args:
        prompt: The prompt to send to Ollama
        config: Configuration dict with ollama_url and ollama_model
        parse_json_fn: Function to parse JSON from response text (optional)
        explain_error_fn: Function to explain HTTP errors (optional)
        report_error_fn: Function to report anonymous errors (optional)

    Returns:
        Parsed JSON response dict, or None on failure
    """
    try:
        ollama_url = config.get('ollama_url', DEFAULT_OLLAMA_URL)
        model = config.get('ollama_model', DEFAULT_OLLAMA_MODEL)

        # Ollama's generate endpoint
        resp = requests.post(
            f"{ollama_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1
                }
            },
            timeout=120  # Local models can be slower, especially on first load
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get('response', '')
            if text:
                if parse_json_fn:
                    return parse_json_fn(text)
                return {'raw_response': text}
        elif resp.status_code == 404:
            logger.error(f"Ollama: Model '{model}' not found. Run: ollama pull {model}")
        else:
            if explain_error_fn:
                error_msg = explain_error_fn(resp.status_code, "Ollama")
                logger.warning(f"Ollama: {error_msg}")
            else:
                logger.warning(f"Ollama: HTTP {resp.status_code}")
            try:
                detail = resp.json().get('error', '')
                if detail:
                    logger.warning(f"Ollama detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("Ollama: Request timed out after 120 seconds - model may still be loading")
        if report_error_fn:
            report_error_fn("Ollama timeout after 120 seconds", context="ollama_api")
    except requests.exceptions.ConnectionError:
        ollama_url = config.get('ollama_url', DEFAULT_OLLAMA_URL)
        logger.error(f"Ollama: Connection failed - is Ollama running at {ollama_url}?")
        if report_error_fn:
            report_error_fn("Ollama connection failed", context="ollama_api")
    except Exception as e:
        logger.error(f"Ollama: {e}")
        if report_error_fn:
            report_error_fn(f"Ollama error: {e}", context="ollama_api")
    return None


def call_ollama_simple(prompt, config, parse_json_fn=None):
    """
    Simple Ollama call for localization queries.

    This is a lightweight version with shorter timeout and minimal error handling,
    suitable for non-critical queries like language localization.

    Args:
        prompt: The prompt to send to Ollama
        config: Configuration dict with ollama_url and ollama_model
        parse_json_fn: Function to parse JSON from response text (optional)

    Returns:
        Parsed JSON response dict, or None on failure
    """
    try:
        ollama_url = config.get('ollama_url', DEFAULT_OLLAMA_URL)
        model = config.get('ollama_model', DEFAULT_OLLAMA_MODEL)
        resp = requests.post(
            f"{ollama_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1}
            },
            timeout=60
        )
        if resp.status_code == 200:
            text = resp.json().get("response", "")
            if text:
                if parse_json_fn:
                    return parse_json_fn(text)
                return {'raw_response': text}
    except Exception as e:
        logger.debug(f"Ollama localization error: {e}")
    return None


def get_ollama_models(config):
    """
    Fetch list of available models from Ollama server.

    Args:
        config: Configuration dict with ollama_url

    Returns:
        List of model names (strings), or empty list on failure
    """
    try:
        ollama_url = config.get('ollama_url', DEFAULT_OLLAMA_URL)
        resp = requests.get(f"{ollama_url}/api/tags", timeout=10)
        if resp.status_code == 200:
            models = resp.json().get('models', [])
            return [m.get('name', '') for m in models if m.get('name')]
        return []
    except:
        return []


def test_ollama_connection(config):
    """
    Test connection to Ollama server.

    Args:
        config: Configuration dict with ollama_url

    Returns:
        Dict with 'success' (bool), and either 'models'/'model_count' or 'error'
    """
    ollama_url = config.get('ollama_url', DEFAULT_OLLAMA_URL)
    try:
        resp = requests.get(f"{ollama_url}/api/tags", timeout=10)
        if resp.status_code == 200:
            models = resp.json().get('models', [])
            return {
                'success': True,
                'models': [m.get('name', '') for m in models],
                'model_count': len(models)
            }
        return {'success': False, 'error': f'HTTP {resp.status_code}'}
    except requests.exceptions.ConnectionError:
        return {'success': False, 'error': f'Cannot connect to {ollama_url}'}
    except requests.exceptions.Timeout:
        return {'success': False, 'error': 'Connection timed out'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


__all__ = [
    'DEFAULT_OLLAMA_URL',
    'DEFAULT_OLLAMA_MODEL',
    'call_ollama',
    'call_ollama_simple',
    'get_ollama_models',
    'test_ollama_connection',
]
