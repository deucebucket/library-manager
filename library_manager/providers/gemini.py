"""Gemini AI provider for Library Manager.

This module provides access to Google's Gemini API for:
- Text-based book identification (call_gemini)
- Audio analysis for credits/narrator extraction (analyze_audio_with_gemini)
- Audio language detection (detect_audio_language)
- Content identification from story audio (_try_gemini_content_identification)
"""

import os
import re
import json
import time
import base64
import logging
import requests

from library_manager.providers.rate_limiter import (
    rate_limit_wait,
    is_circuit_open,
    record_api_failure,
    record_api_success,
)

logger = logging.getLogger(__name__)

# Gemini API base URL
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models"

# Default models
DEFAULT_TEXT_MODEL = "gemini-2.0-flash"
DEFAULT_AUDIO_MODEL = "gemini-2.5-flash"


def _call_gemini_simple(prompt, config, parse_json_response_fn):
    """Simple Gemini call for localization queries.

    Args:
        prompt: The prompt to send to Gemini
        config: Config dict with gemini_api_key and gemini_model
        parse_json_response_fn: Function to parse JSON from AI response

    Returns:
        Parsed JSON response or None
    """
    # Check circuit breaker
    if is_circuit_open('gemini'):
        return None
    rate_limit_wait('gemini')

    try:
        api_key = config.get('gemini_api_key')
        model = config.get('gemini_model', DEFAULT_TEXT_MODEL)
        resp = requests.post(
            f"{GEMINI_API_URL}/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=30
        )
        if resp.status_code == 200:
            text = resp.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            return parse_json_response_fn(text) if text else None
    except Exception as e:
        logger.debug(f"Gemini localization error: {e}")
    return None


def call_gemini(prompt, config, retry_count=0, parse_json_response_fn=None,
                explain_http_error_fn=None, report_anonymous_error_fn=None):
    """Call Google Gemini API directly with automatic retry on rate limit.

    Args:
        prompt: The prompt to send to Gemini
        config: Config dict with gemini_api_key and gemini_model
        retry_count: Internal retry counter (do not set manually)
        parse_json_response_fn: Function to parse JSON from AI response
        explain_http_error_fn: Function to convert HTTP status to message
        report_anonymous_error_fn: Function to report errors anonymously

    Returns:
        Parsed JSON response or None
    """
    # Check circuit breaker
    if is_circuit_open('gemini'):
        logger.debug("[GEMINI] Circuit breaker open, skipping")
        return None

    # Respect rate limits (10 RPM free tier as of Jan 2026)
    rate_limit_wait('gemini')

    try:
        api_key = config.get('gemini_api_key')
        model = config.get('gemini_model', DEFAULT_TEXT_MODEL)

        resp = requests.post(
            f"{GEMINI_API_URL}/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=90
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                # Issue #57: Log AI response for debugging hallucinations
                logger.debug(f"Gemini raw response: {text[:500]}{'...' if len(text) > 500 else ''}")
                parsed = parse_json_response_fn(text) if parse_json_response_fn else None
                if parsed:
                    # Log the parsed author/title for traceability
                    if isinstance(parsed, list):
                        for item in parsed[:3]:  # Log first 3 items
                            logger.info(f"Gemini parsed: {item.get('author', '?')} - {item.get('title', '?')}")
                    elif isinstance(parsed, dict):
                        logger.info(f"Gemini parsed: {parsed.get('author', parsed.get('recommended_author', '?'))} - {parsed.get('title', parsed.get('recommended_title', '?'))}")
                return parsed
        elif resp.status_code == 429 and retry_count < 3:
            # Rate limit - parse retry time and wait
            error_msg = explain_http_error_fn(resp.status_code, "Gemini") if explain_http_error_fn else f"HTTP {resp.status_code}"
            logger.warning(f"Gemini: {error_msg}")
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"Gemini detail: {detail}")
                    # Check if this is a daily quota exceeded (not just per-minute rate limit)
                    if 'quota' in detail.lower() and ('limit: 0' in detail or 'exceeded' in detail.lower()):
                        logger.warning("[GEMINI] Daily quota exhausted - tripping circuit breaker")
                        record_api_failure('gemini')
                        record_api_failure('gemini')  # Trip immediately
                        return None
                    # Try to parse "Please retry in X.XXXs" from message
                    match = re.search(r'retry in (\d+\.?\d*)s', detail)
                    if match:
                        wait_time = float(match.group(1)) + 5  # Add 5 sec buffer
                        logger.info(f"Gemini: Waiting {wait_time:.0f} seconds before retry...")
                        time.sleep(wait_time)
                        return call_gemini(prompt, config, retry_count + 1,
                                          parse_json_response_fn, explain_http_error_fn, report_anonymous_error_fn)
            except:
                pass
            # Default wait if we can't parse the time
            wait_time = 45 * (retry_count + 1)
            logger.info(f"Gemini: Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
            return call_gemini(prompt, config, retry_count + 1,
                              parse_json_response_fn, explain_http_error_fn, report_anonymous_error_fn)
        else:
            error_msg = explain_http_error_fn(resp.status_code, "Gemini") if explain_http_error_fn else f"HTTP {resp.status_code}"
            logger.warning(f"Gemini: {error_msg}")
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"Gemini detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("Gemini: Request timed out after 90 seconds")
        if report_anonymous_error_fn:
            report_anonymous_error_fn("Gemini timeout after 90 seconds", context="gemini_api")
    except requests.exceptions.ConnectionError:
        logger.error("Gemini: Connection failed - check your internet")
        if report_anonymous_error_fn:
            report_anonymous_error_fn("Gemini connection failed", context="gemini_api")
    except Exception as e:
        logger.error(f"Gemini: {e}")
        if report_anonymous_error_fn:
            report_anonymous_error_fn(f"Gemini error: {e}", context="gemini_api")
    return None


def analyze_audio_with_gemini(audio_file, config, duration=90, mode='credits',
                               extract_audio_sample_fn=None, parse_json_response_fn=None):
    """
    Send audio sample to Gemini for analysis.

    Modes:
    - 'credits': Optimized for first file with opening credits (title/author/narrator announcement)
    - 'identify': For any chapter file - extracts chapter info and any identifying details

    Args:
        audio_file: Path to the audio file
        config: App config with Gemini API key
        duration: Seconds of audio to analyze (default 90, use 45 for credits)
        mode: 'credits' or 'identify'
        extract_audio_sample_fn: Function to extract audio sample from file
        parse_json_response_fn: Function to parse JSON from AI response

    Returns dict with extracted info or None on failure.
    """
    # Check circuit breaker
    if is_circuit_open('gemini'):
        logger.debug("[GEMINI AUDIO] Circuit breaker open, skipping")
        return None

    api_key = config.get('gemini_api_key')
    if not api_key:
        return None

    if not extract_audio_sample_fn:
        logger.error("[GEMINI AUDIO] No extract_audio_sample function provided")
        return None

    # Extract audio sample
    sample_path = extract_audio_sample_fn(audio_file, duration_seconds=duration)
    if not sample_path:
        logger.debug(f"Could not extract audio sample from {audio_file}")
        return None

    try:
        # Read and encode audio
        with open(sample_path, 'rb') as f:
            audio_data = base64.b64encode(f.read()).decode('utf-8')

        # Clean up temp file
        os.unlink(sample_path)

        # Use gemini-2.5-flash for audio (separate quota from text analysis model)
        model = DEFAULT_AUDIO_MODEL

        # Different prompts for different analysis modes
        if mode == 'identify':
            # For orphan/misplaced files - need to identify what book this chapter belongs to
            prompt = """Listen to this audiobook chapter and extract ANY identifying information.
This may be a chapter from the middle of an audiobook, not necessarily the beginning.

Narrators often announce the chapter number at the start of each chapter.
Listen carefully for:
- Chapter number or part number ("Chapter 12", "Part 3")
- Book title (sometimes mentioned even in middle chapters)
- Author name (sometimes mentioned)
- Narrator name (sometimes mentioned)
- Series name (e.g., "Book 3 of the Hunger Games series")
- Character names (can help identify the book)
- Any other identifying context clues

Return in JSON format:
{
    "title": "book title if mentioned anywhere",
    "author": "author name if mentioned",
    "narrator": "narrator name if mentioned",
    "series": "series name if mentioned",
    "chapter_number": "chapter or part number if announced (e.g., '12', '3.5')",
    "chapter_title": "chapter title if announced (e.g., 'The Battle Begins')",
    "language": "ISO 639-1 code of spoken language (en, de, fr, es, etc.)",
    "character_names": ["list", "of", "character", "names", "heard"],
    "context_clues": "any other identifying info that could help identify the book",
    "confidence": "high/medium/low"
}

If information is not clearly stated, use null. Do not guess - only report what you hear."""
        else:
            # Default 'credits' mode - for first file with opening credits
            prompt = """Listen to this audiobook intro and extract the following information.
Audiobooks typically start with an announcement like "This is [Title] by [Author], read by [Narrator]".
This announcement is usually in the first 30-60 seconds.

Extract and return in JSON format:
{
    "title": "book title if mentioned",
    "author": "author name if mentioned",
    "narrator": "narrator name if mentioned",
    "series": "series name if mentioned",
    "language": "ISO 639-1 code of the spoken language (e.g., en, de, fr, es)",
    "confidence": "high/medium/low based on how clearly the info was stated"
}

For the language field:
- Listen to what language the narrator is speaking
- Use the ISO 639-1 two-letter code (en=English, de=German, fr=French, es=Spanish, etc.)
- This should reflect the SPOKEN language, not the original book language

If information is not clearly stated in the audio, use null for that field.
Only include information you actually heard - do not guess."""

        # Respect rate limits before making the call
        rate_limit_wait('gemini')

        resp = requests.post(
            f"{GEMINI_API_URL}/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {
                            "inline_data": {
                                "mime_type": "audio/mp3",
                                "data": audio_data
                            }
                        }
                    ]
                }],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=120  # Audio processing can take longer
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                parsed = parse_json_response_fn(text) if parse_json_response_fn else None
                if parsed:
                    logger.info(f"Audio analysis extracted: {parsed}")
                    record_api_success('gemini')
                    return parsed
        elif resp.status_code == 429:
            # Rate limit or quota exceeded
            detail = resp.text[:500]
            logger.warning(f"Gemini audio API rate limited: {detail}")
            if 'quota' in detail.lower() and ('limit: 0' in detail or 'exceeded' in detail.lower()):
                logger.warning("[GEMINI AUDIO] Daily quota exhausted - tripping circuit breaker")
                record_api_failure('gemini')
                record_api_failure('gemini')
        else:
            logger.debug(f"Gemini audio API error {resp.status_code}: {resp.text[:200]}")

    except Exception as e:
        logger.debug(f"Audio analysis error: {e}")
        # Clean up temp file if it exists
        if sample_path and os.path.exists(sample_path):
            os.unlink(sample_path)

    return None


def detect_audio_language(audio_file, config, extract_audio_sample_fn=None, parse_json_response_fn=None):
    """
    Detect the spoken language from an audio file using Gemini.
    This is a lightweight version of analyze_audio_with_gemini focused only on language.

    Args:
        audio_file: Path to audio file
        config: App config with Gemini API key
        extract_audio_sample_fn: Function to extract audio sample from file
        parse_json_response_fn: Function to parse JSON from AI response

    Returns:
        dict with 'language' (ISO 639-1 code), 'language_name', and 'confidence', or None
    """
    api_key = config.get('gemini_api_key')
    if not api_key:
        logger.debug("No Gemini API key for audio language detection")
        return None

    if not extract_audio_sample_fn:
        logger.error("[GEMINI AUDIO] No extract_audio_sample function provided")
        return None

    # Extract shorter audio sample (30 seconds is enough for language detection)
    sample_path = extract_audio_sample_fn(audio_file, duration_seconds=30)
    if not sample_path:
        logger.debug(f"Could not extract audio sample from {audio_file}")
        return None

    try:
        with open(sample_path, 'rb') as f:
            audio_data = base64.b64encode(f.read()).decode('utf-8')

        os.unlink(sample_path)

        model = DEFAULT_AUDIO_MODEL

        prompt = """Listen to this audiobook sample and identify the spoken language.

Return JSON only:
{
    "language": "ISO 639-1 two-letter code (en, de, fr, es, it, pt, nl, sv, no, da, fi, pl, ru, ja, zh, ko, etc.)",
    "language_name": "Full language name (English, German, French, etc.)",
    "confidence": "high/medium/low"
}

Focus on the SPOKEN language you hear in the narration, not any background music or sound effects."""

        resp = requests.post(
            f"{GEMINI_API_URL}/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "audio/mp3", "data": audio_data}}
                    ]
                }],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=60
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                parsed = parse_json_response_fn(text) if parse_json_response_fn else None
                if parsed and parsed.get('language'):
                    logger.info(f"Audio language detected: {parsed.get('language_name', parsed['language'])} ({parsed.get('confidence', 'unknown')} confidence)")
                    return parsed
        else:
            logger.debug(f"Gemini audio language API error {resp.status_code}")

    except Exception as e:
        logger.debug(f"Audio language detection error: {e}")
        if sample_path and os.path.exists(sample_path):
            os.unlink(sample_path)

    return None


def try_gemini_content_identification(sample_path, api_key, parse_json_response_fn=None):
    """Try Gemini Audio API for content identification. Returns result or None.

    This function analyzes story CONTENT (not credits) to identify books from
    plot, characters, and writing style.

    Args:
        sample_path: Path to the audio sample file (already extracted)
        api_key: Gemini API key
        parse_json_response_fn: Function to parse JSON from AI response (optional, uses internal parsing if not provided)

    Returns:
        dict with title, author, series, confidence, etc. or None
    """
    # Check circuit breaker
    if is_circuit_open('gemini'):
        logger.debug("[LAYER 4] Gemini circuit breaker open, skipping")
        return None

    # Respect rate limits
    rate_limit_wait('gemini')

    try:
        with open(sample_path, 'rb') as f:
            audio_data = base64.b64encode(f.read()).decode('utf-8')

        model = DEFAULT_AUDIO_MODEL

        prompt = """Listen to this audiobook excerpt and perform these tasks:

1. TRANSCRIBE: Write out exactly what you hear - the actual story text being narrated.
   Get at least 2-3 sentences of the actual story content.

2. IDENTIFY: Based on the transcribed content, identify what book this is from.
   Look for:
   - Character names (protagonists, antagonists, places)
   - Plot elements and events
   - Writing style and genre
   - Any unique phrases or dialogue
   - Setting details

3. SEARCH YOUR KNOWLEDGE: Match the content against known books.
   Consider:
   - Famous novels and their scenes
   - Popular audiobook series
   - Genre conventions (fantasy names, sci-fi terminology, etc.)

Return in JSON format:
{
    "transcription": "The exact text you heard from the audiobook (2-3 sentences minimum)",
    "title": "identified book title (or best guess)",
    "author": "identified author name (or best guess)",
    "series": "series name if applicable",
    "series_num": "book number in series if known",
    "narrator": "narrator if you can identify their voice style",
    "genre": "detected genre (fantasy, sci-fi, thriller, romance, etc.)",
    "confidence": "high/medium/low",
    "reasoning": "brief explanation of how you identified the book"
}

Even if you're not 100% certain, provide your best guess with appropriate confidence level.
Use character names, plot elements, and writing style as clues."""

        resp = requests.post(
            f"{GEMINI_API_URL}/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {
                            "inline_data": {
                                "mime_type": "audio/mp3",
                                "data": audio_data
                            }
                        }
                    ]
                }],
                "generationConfig": {
                    "temperature": 0.2,
                    "maxOutputTokens": 1024
                }
            },
            timeout=90
        )

        if resp.status_code == 429:
            detail = resp.text[:500]
            logger.warning(f"[LAYER 4] Gemini rate limited (429): {detail}")
            # Check if daily quota is exhausted
            if 'quota' in detail.lower() and ('limit: 0' in detail or 'exceeded' in detail.lower()):
                logger.warning("[LAYER 4] Gemini daily quota exhausted - tripping circuit breaker")
                record_api_failure('gemini')
                record_api_failure('gemini')
            return None

        if resp.status_code != 200:
            logger.warning(f"[LAYER 4] Gemini API failed: {resp.status_code} - {resp.text[:200]}")
            return None

        data = resp.json()
        logger.debug(f"[LAYER 4] Gemini response received")

        # Extract the text response
        try:
            text = data['candidates'][0]['content']['parts'][0]['text']
            logger.debug(f"[LAYER 4] Gemini text response: {text[:200]}")

            # Parse JSON from response
            if parse_json_response_fn:
                result = parse_json_response_fn(text)
            else:
                # Internal JSON parsing as fallback
                json_match = re.search(r'\{[\s\S]*\}', text)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    result = None

            if result:
                # Log what we found
                if result.get('title') and result.get('author'):
                    logger.info(f"[LAYER 4] Content identified: {result.get('author')}/{result.get('title')} "
                               f"(confidence: {result.get('confidence')}, reason: {result.get('reasoning', 'N/A')[:50]})")
                return result
            else:
                logger.warning(f"[LAYER 4] No JSON found in Gemini response: {text[:200]}")
                return None
        except Exception as e:
            logger.warning(f"[LAYER 4] Failed to parse Gemini content response: {e} - text: {text[:100] if 'text' in dir() else 'N/A'}")
            return None

    except Exception as e:
        logger.warning(f"[LAYER 4] Content identification error: {e}")
        return None

    return None


__all__ = [
    'GEMINI_API_URL',
    'DEFAULT_TEXT_MODEL',
    'DEFAULT_AUDIO_MODEL',
    '_call_gemini_simple',
    'call_gemini',
    'analyze_audio_with_gemini',
    'detect_audio_language',
    'try_gemini_content_identification',
]
