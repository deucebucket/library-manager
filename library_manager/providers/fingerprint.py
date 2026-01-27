"""Audio fingerprinting provider for Library Manager.

Generates acoustic fingerprints using Chromaprint/fpcalc for instant
audiobook identification. Fingerprints survive re-encoding, bitrate
changes, and format conversion.

Part of Issue #78 - Audio Fingerprinting + Narrator Voice ID
Based on @Merijeek's idea from Issue #72
"""

import os
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import requests

logger = logging.getLogger(__name__)

# BookDB fingerprint endpoints
BOOKDB_FINGERPRINT_URL = "https://bookdb.deucebucket.com/api/fingerprint"

# Default fingerprint duration (seconds)
DEFAULT_DURATION = 120  # 2 minutes


def is_fpcalc_available() -> bool:
    """Check if fpcalc (Chromaprint CLI) is installed."""
    try:
        result = subprocess.run(
            ['fpcalc', '-version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


def generate_fingerprint(
    audio_path: str,
    duration: int = DEFAULT_DURATION,
    start_offset: int = 0
) -> Optional[Tuple[str, int]]:
    """
    Generate acoustic fingerprint for an audio file.

    Args:
        audio_path: Path to audio file (m4b, mp3, flac, etc.)
        duration: Seconds of audio to fingerprint (default 120)
        start_offset: Seconds to skip from start (for skipping intros)

    Returns:
        Tuple of (fingerprint_string, actual_duration) or None on failure
    """
    if not os.path.exists(audio_path):
        logger.error(f"[FINGERPRINT] File not found: {audio_path}")
        return None

    if not is_fpcalc_available():
        logger.warning("[FINGERPRINT] fpcalc not installed - cannot generate fingerprint")
        return None

    try:
        # Build fpcalc command
        cmd = ['fpcalc', '-length', str(duration)]

        # Add start offset if specified (requires ffmpeg, fpcalc handles it)
        if start_offset > 0:
            cmd.extend(['-raw', '-signed'])  # Raw mode for offset support

        cmd.append(audio_path)

        logger.debug(f"[FINGERPRINT] Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60  # 1 minute timeout
        )

        if result.returncode != 0:
            logger.error(f"[FINGERPRINT] fpcalc failed: {result.stderr}")
            return None

        # Parse output
        fingerprint = None
        actual_duration = 0

        for line in result.stdout.strip().split('\n'):
            if line.startswith('FINGERPRINT='):
                fingerprint = line.split('=', 1)[1]
            elif line.startswith('DURATION='):
                actual_duration = int(line.split('=', 1)[1])

        if not fingerprint:
            logger.error("[FINGERPRINT] No fingerprint in fpcalc output")
            return None

        logger.info(f"[FINGERPRINT] Generated fingerprint ({len(fingerprint)} chars) for {actual_duration}s of audio")
        return (fingerprint, actual_duration)

    except subprocess.TimeoutExpired:
        logger.error("[FINGERPRINT] fpcalc timed out")
        return None
    except Exception as e:
        logger.error(f"[FINGERPRINT] Error generating fingerprint: {e}")
        return None


def lookup_fingerprint(
    fingerprint: str,
    api_key: Optional[str] = None,
    threshold: float = 0.8
) -> Optional[Dict[str, Any]]:
    """
    Look up a fingerprint in BookDB.

    Args:
        fingerprint: The Chromaprint fingerprint string
        api_key: Optional BookDB API key for higher rate limits

    Returns:
        Book metadata dict if match found, None otherwise
    """
    try:
        headers = {}
        if api_key:
            headers['X-API-Key'] = api_key

        response = requests.get(
            f"{BOOKDB_FINGERPRINT_URL}/lookup",
            params={'fp': fingerprint, 'threshold': threshold},
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            if data.get('match'):
                logger.info(f"[FINGERPRINT] Match found: {data.get('book', {}).get('title')} "
                           f"(confidence: {data.get('confidence', 0):.2f})")
                return data.get('book')
            else:
                logger.debug("[FINGERPRINT] No match in database")
                return None

        elif response.status_code == 404:
            logger.debug("[FINGERPRINT] Fingerprint not in database")
            return None

        else:
            logger.warning(f"[FINGERPRINT] Lookup failed: {response.status_code}")
            return None

    except requests.RequestException as e:
        logger.error(f"[FINGERPRINT] Lookup error: {e}")
        return None


def contribute_fingerprint(
    fingerprint: str,
    duration: int,
    book_metadata: Dict[str, Any],
    api_key: Optional[str] = None
) -> bool:
    """
    Contribute a fingerprint to BookDB.

    Args:
        fingerprint: The Chromaprint fingerprint string
        duration: Duration of audio that was fingerprinted
        book_metadata: Dict with author, title, narrator, series, etc.
        api_key: Optional BookDB API key

    Returns:
        True if contribution successful, False otherwise
    """
    try:
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['X-API-Key'] = api_key

        payload = {
            'fingerprint': fingerprint,
            'duration': duration,
            'author': book_metadata.get('author', ''),
            'title': book_metadata.get('title', ''),
            'narrator': book_metadata.get('narrator', ''),
            'series': book_metadata.get('series', ''),
            'series_position': book_metadata.get('series_position', ''),
        }

        response = requests.post(
            BOOKDB_FINGERPRINT_URL,
            json=payload,
            headers=headers,
            timeout=10
        )

        if response.status_code in (200, 201):
            data = response.json()
            if data.get('is_new'):
                logger.info(f"[FINGERPRINT] Contributed new fingerprint for: {book_metadata.get('title')}")
            else:
                logger.info(f"[FINGERPRINT] Confirmed existing fingerprint for: {book_metadata.get('title')}")
            return True
        else:
            logger.warning(f"[FINGERPRINT] Contribution failed: {response.status_code}")
            return False

    except requests.RequestException as e:
        logger.error(f"[FINGERPRINT] Contribution error: {e}")
        return False


def identify_by_fingerprint(
    audio_path: str,
    api_key: Optional[str] = None,
    duration: int = DEFAULT_DURATION
) -> Optional[Dict[str, Any]]:
    """
    Attempt to identify an audiobook by its audio fingerprint.

    This is the main entry point - generates fingerprint and looks it up.

    Args:
        audio_path: Path to audio file
        api_key: Optional BookDB API key
        duration: Seconds of audio to fingerprint

    Returns:
        Book metadata dict if match found, None otherwise
    """
    # Generate fingerprint
    result = generate_fingerprint(audio_path, duration=duration)
    if not result:
        return None

    fingerprint, actual_duration = result

    # Look up in BookDB
    return lookup_fingerprint(fingerprint, api_key=api_key)


# ============== NARRATOR VOICE ID (Phase 2) ==============
#
# Future implementation will add:
# - extract_voice_embedding(audio_path) -> vector
# - lookup_narrator(embedding) -> narrator name
# - contribute_narrator(embedding, name) -> bool
#
# Requires: resemblyzer or speechbrain for speaker embeddings
# =========================================================
