"""OpenRouter AI provider for Library Manager.

This module provides access to OpenRouter's AI models for:
- Book identification from prompts
- Transcript-based identification
- Simple localization queries

Free tier limits (as of Jan 2026):
- 20 requests/minute
- Daily limits vary by model
"""

import re
import json
import time
import logging
import requests

from library_manager.providers.rate_limiter import (
    rate_limit_wait,
    is_circuit_open,
    record_api_failure,
    record_api_success,
    API_CIRCUIT_BREAKER,
)

logger = logging.getLogger(__name__)

# OpenRouter API endpoint
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Default model for OpenRouter
DEFAULT_MODEL = "xiaomi/mimo-v2-flash:free"


def _explain_http_error(status_code, provider):
    """Convert HTTP status codes to human-readable errors."""
    errors = {
        400: "Bad request - the API didn't understand our request",
        401: "Invalid API key - check your key in Settings",
        403: "Access denied - your API key doesn't have permission",
        404: "Model not found - the selected model may not exist",
        429: "Rate limit exceeded - too many requests, waiting before retry",
        500: f"{provider} server error - their servers are having issues",
        502: f"{provider} is temporarily down - try again later",
        503: f"{provider} is overloaded - try again in a few minutes",
    }
    return errors.get(status_code, f"Unknown error (HTTP {status_code})")


def _parse_json_response(text):
    """Extract JSON from AI response - handles various AI response formats."""
    text = text.strip()

    # Remove markdown code blocks
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Extract first JSON object using regex (handles extra text before/after)
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    # Try extracting JSON array
    array_match = re.search(r'\[.*?\]', text, re.DOTALL)
    if array_match:
        try:
            return json.loads(array_match.group())
        except json.JSONDecodeError:
            pass

    return None


def call_openrouter(prompt, config, error_reporter=None):
    """
    Call OpenRouter API with circuit breaker for daily limits.

    Args:
        prompt: The prompt to send to the model
        config: Configuration dict with openrouter_api_key, openrouter_model
        error_reporter: Optional function to report errors (signature: error_msg, context=None)

    Returns:
        Parsed JSON response or None on failure
    """
    # Check circuit breaker - skip if we've hit daily limits
    cb = API_CIRCUIT_BREAKER.get('openrouter', {})
    if cb.get('circuit_open_until', 0) > time.time():
        remaining = int(cb['circuit_open_until'] - time.time())
        logger.debug(f"OpenRouter: Circuit OPEN, skipping (cooldown: {remaining}s remaining)")
        return None

    rate_limit_wait('openrouter')  # Respect free tier limits

    try:
        resp = requests.post(
            OPENROUTER_API_URL,
            headers={
                "Authorization": f"Bearer {config['openrouter_api_key']}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/deucebucket/library-manager",
                "X-Title": "Library Metadata Manager"
            },
            json={
                "model": config.get('openrouter_model', DEFAULT_MODEL),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1
            },
            timeout=90
        )

        if resp.status_code == 200:
            # Reset circuit breaker on success
            record_api_success('openrouter')
            result = resp.json()
            text = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if text:
                # Issue #57: Log AI response for debugging hallucinations
                logger.debug(f"OpenRouter raw response: {text[:500]}{'...' if len(text) > 500 else ''}")
                parsed = _parse_json_response(text)
                if parsed:
                    # Log the parsed author/title for traceability
                    if isinstance(parsed, list):
                        for item in parsed[:3]:  # Log first 3 items
                            logger.info(f"OpenRouter parsed: {item.get('author', '?')} - {item.get('title', '?')}")
                    elif isinstance(parsed, dict):
                        logger.info(f"OpenRouter parsed: {parsed.get('author', parsed.get('recommended_author', '?'))} - {parsed.get('title', parsed.get('recommended_title', '?'))}")
                return parsed
        elif resp.status_code == 429:
            # Rate limit - check if it's daily limit
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if 'free-models-per-day' in detail.lower() or 'daily' in detail.lower():
                    # Daily limit hit - open circuit breaker for 1 hour
                    logger.warning(f"OpenRouter: Daily limit reached, backing off for 1 hour")
                    API_CIRCUIT_BREAKER['openrouter']['failures'] = cb.get('max_failures', 2)
                    API_CIRCUIT_BREAKER['openrouter']['circuit_open_until'] = time.time() + cb.get('cooldown', 3600)
                else:
                    logger.warning(f"OpenRouter: Rate limited - {detail}")
            except:
                logger.warning("OpenRouter: Rate limited")
        else:
            error_msg = _explain_http_error(resp.status_code, "OpenRouter")
            logger.warning(f"OpenRouter: {error_msg}")
            # Try to get more detail from response
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"OpenRouter detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("OpenRouter: Request timed out after 90 seconds")
        if error_reporter:
            error_reporter("OpenRouter timeout after 90 seconds", context="openrouter_api")
    except requests.exceptions.ConnectionError:
        logger.error("OpenRouter: Connection failed - check your internet")
        if error_reporter:
            error_reporter("OpenRouter connection failed", context="openrouter_api")
    except Exception as e:
        logger.error(f"OpenRouter: {e}")
        if error_reporter:
            error_reporter(f"OpenRouter error: {e}", context="openrouter_api")
    return None


def call_openrouter_simple(prompt, config):
    """
    Simple OpenRouter call for localization queries.

    This is a lightweight version without circuit breaker tracking,
    used for simple localization/translation tasks.

    Args:
        prompt: The prompt to send to the model
        config: Configuration dict with openrouter_api_key, openrouter_model

    Returns:
        Parsed JSON response or None on failure
    """
    rate_limit_wait('openrouter')  # Respect free tier limits
    try:
        resp = requests.post(
            OPENROUTER_API_URL,
            headers={
                "Authorization": f"Bearer {config['openrouter_api_key']}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.get('openrouter_model', 'google/gemma-3n-e4b-it:free'),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1
            },
            timeout=30
        )
        if resp.status_code == 200:
            text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            return _parse_json_response(text) if text else None
    except Exception as e:
        logger.debug(f"OpenRouter localization error: {e}")
    return None


def identify_book_from_transcript(transcript, config):
    """
    Use OpenRouter to identify a book from transcribed text.

    This is a fallback when other audio identification methods fail.
    The AI analyzes the transcript content to identify the book based on
    character names, plot elements, and writing style.

    Args:
        transcript: The transcribed text from the audiobook
        config: Configuration dict with openrouter_api_key, layer4_openrouter_model

    Returns:
        dict with title, author, series, confidence, etc. or None
    """
    api_key = config.get('openrouter_api_key')
    if not api_key:
        logger.warning("[LAYER 4] No OpenRouter API key for transcript identification")
        return None

    # Respect free tier rate limits (20 req/min + daily limits)
    rate_limit_wait('openrouter')

    # Use a capable free model for book identification
    # Options: xiaomi/mimo-v2-flash:free (262K ctx), allenai/molmo-2-8b:free, mistralai/devstral-2512:free
    model = config.get('layer4_openrouter_model', 'xiaomi/mimo-v2-flash:free')

    prompt = f"""You are a literary expert. Based on this audiobook transcript excerpt, identify the book.

TRANSCRIPT:
"{transcript[:2000]}"

Analyze:
1. Character names mentioned
2. Plot elements and events
3. Writing style and genre markers
4. Setting details
5. Any unique phrases or dialogue

Return ONLY valid JSON (no markdown):
{{
    "title": "identified book title (or best guess)",
    "author": "identified author name (or best guess)",
    "series": "series name if applicable, or null",
    "series_num": "book number if known, or null",
    "narrator": null,
    "genre": "detected genre",
    "confidence": "high/medium/low",
    "reasoning": "brief explanation of how you identified the book"
}}"""

    try:
        resp = requests.post(
            OPENROUTER_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/deucebucket/library-manager",
                "X-Title": "Library Manager"
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
                "max_tokens": 1024
            },
            timeout=60
        )

        if resp.status_code != 200:
            logger.warning(f"[LAYER 4] OpenRouter failed: {resp.status_code} - {resp.text[:200]}")
            return None

        data = resp.json()
        text = data['choices'][0]['message']['content']

        # Parse JSON from response
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            result = json.loads(json_match.group())

            if result.get('title') and result.get('author'):
                logger.info(f"[LAYER 4] OpenRouter identified: {result.get('author')}/{result.get('title')} "
                           f"(confidence: {result.get('confidence')})")
            return result
        else:
            logger.warning(f"[LAYER 4] No JSON in OpenRouter response: {text[:200]}")
            return None

    except Exception as e:
        logger.warning(f"[LAYER 4] OpenRouter identification error: {e}")
        return None


def test_openrouter_connection(api_key, model=None):
    """
    Test OpenRouter API connection.

    Args:
        api_key: The OpenRouter API key to test
        model: Optional model to test with (defaults to gemma-3n-e4b-it:free)

    Returns:
        dict with 'success', 'model', 'message' or 'error' keys
    """
    if not api_key:
        return {
            'success': False,
            'error': 'No OpenRouter API key configured'
        }

    try:
        model = model or 'google/gemma-3n-e4b-it:free'
        resp = requests.post(
            OPENROUTER_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": "Reply with just the word 'connected'"}],
                "max_tokens": 10
            },
            timeout=10
        )

        if resp.status_code == 200:
            return {
                'success': True,
                'model': model,
                'message': 'OpenRouter API connected'
            }
        else:
            error_data = resp.json()
            error_msg = error_data.get('error', {}).get('message', f'Status {resp.status_code}')
            return {
                'success': False,
                'error': error_msg
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


__all__ = [
    'OPENROUTER_API_URL',
    'DEFAULT_MODEL',
    'call_openrouter',
    'call_openrouter_simple',
    'identify_book_from_transcript',
    'test_openrouter_connection',
]
