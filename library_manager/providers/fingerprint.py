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

from library_manager.providers.rate_limiter import handle_rate_limit_response

logger = logging.getLogger(__name__)

# Skaldleita fingerprint endpoints
# Skaldleita = "Shazam for audiobooks" - instant identification via audio fingerprint + voice ID
SKALDLEITA_BASE_URL = "https://skaldleita.com"
SKALDLEITA_FINGERPRINT_URL = f"{SKALDLEITA_BASE_URL}/api/fingerprint"

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
    Look up a fingerprint in Skaldleita.

    Args:
        fingerprint: The Chromaprint fingerprint string
        api_key: Optional Skaldleita API key for higher rate limits

    Returns:
        Book metadata dict if match found, None otherwise
    """
    try:
        headers = {}
        if api_key:
            headers['X-API-Key'] = api_key

        response = requests.get(
            f"{SKALDLEITA_FINGERPRINT_URL}/lookup",
            params={'fp': fingerprint, 'threshold': threshold},
            headers=headers,
            timeout=10
        )

        if response.status_code == 429:
            # Fingerprint lookups are supplementary - fail fast, don't retry.
            # The circuit breaker will back off future requests automatically.
            rl = handle_rate_limit_response(response, 'bookdb')
            logger.warning(f"[FINGERPRINT] Rate limited (retry_after: {rl['retry_after']})")
            return None

        if response.status_code == 200:
            data = response.json()
            if data.get('match'):
                # Return book if linked to database, otherwise contributed metadata
                result = data.get('book') or data.get('contributed_metadata')
                if result:
                    result['confidence'] = data.get('confidence', 0)
                    title = result.get('title', 'Unknown')
                    logger.info(f"[FINGERPRINT] Match found: {title} "
                               f"(confidence: {data.get('confidence', 0):.2f})")
                    return result
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
    Contribute a fingerprint to Skaldleita.

    Args:
        fingerprint: The Chromaprint fingerprint string
        duration: Duration of audio that was fingerprinted
        book_metadata: Dict with author, title, narrator, series, etc.
        api_key: Optional Skaldleita API key

    Returns:
        True if contribution successful, False otherwise
    """
    try:
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['X-API-Key'] = api_key

        # Handle series_position - must be float or None, not empty string
        series_pos = book_metadata.get('series_position')
        if series_pos == '' or series_pos is None:
            series_pos = None
        else:
            try:
                series_pos = float(series_pos)
            except (ValueError, TypeError):
                series_pos = None

        payload = {
            'fingerprint': fingerprint,
            'duration': duration,
            'author': book_metadata.get('author', ''),
            'title': book_metadata.get('title', ''),
            'narrator': book_metadata.get('narrator', ''),
            'series': book_metadata.get('series', ''),
            'series_position': series_pos,
        }

        response = requests.post(
            SKALDLEITA_FINGERPRINT_URL,
            json=payload,
            headers=headers,
            timeout=10
        )

        if response.status_code == 429:
            # Contributions are best-effort - fail fast, don't retry
            rl = handle_rate_limit_response(response, 'bookdb')
            logger.warning(f"[FINGERPRINT] Contribution rate limited (retry_after: {rl['retry_after']})")
            return False

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
        api_key: Optional Skaldleita API key
        duration: Seconds of audio to fingerprint

    Returns:
        Book metadata dict if match found, None otherwise
    """
    # Generate fingerprint
    result = generate_fingerprint(audio_path, duration=duration)
    if not result:
        return None

    fingerprint, actual_duration = result

    # Look up in Skaldleita
    return lookup_fingerprint(fingerprint, api_key=api_key)


def generate_dual_fingerprints(
    audio_path: str,
    intro_duration: int = 120,
    middle_offset: int = 600,
    middle_duration: int = 60
) -> Dict[str, Any]:
    """
    Generate fingerprints from both intro and middle of audiobook.

    This gives better coverage for:
    - Books with long music/credits intros
    - Books where narrator intro is several minutes in

    Args:
        audio_path: Path to audio file
        intro_duration: Seconds from start to fingerprint (default 120)
        middle_offset: Seconds into file for middle sample (default 600 = 10 min)
        middle_duration: Seconds for middle sample (default 60)

    Returns:
        Dict with 'intro' and 'middle' fingerprint tuples, or None values
    """
    result = {
        'intro': None,
        'middle': None
    }

    # Generate intro fingerprint (first 2 minutes)
    intro = generate_fingerprint(audio_path, duration=intro_duration, start_offset=0)
    if intro:
        result['intro'] = intro
        logger.info(f"[FINGERPRINT] Generated intro fingerprint ({intro[1]}s)")

    # Generate middle fingerprint (10 min in, 1 min sample)
    # This requires ffmpeg to skip to offset - fpcalc doesn't support it directly
    # For now, just use intro. Middle fingerprinting requires temp file extraction.
    # TODO: Add middle fingerprint extraction via ffmpeg temp file

    return result


def try_fingerprint_identification(
    audio_path: str,
    api_key: Optional[str] = None,
    duration: int = DEFAULT_DURATION
) -> Optional[Dict[str, Any]]:
    """
    Attempt to identify audiobook via fingerprint lookup.

    This should be called BEFORE expensive Whisper/Gemini analysis.
    If a fingerprint match is found, we can skip the AI analysis entirely.

    Args:
        audio_path: Path to audio file
        api_key: Optional Skaldleita API key
        duration: Seconds of audio to fingerprint

    Returns:
        Dict with book metadata if match found, None otherwise
        Includes 'fingerprint' key for later contribution
    """
    if not is_fpcalc_available():
        logger.debug("[FINGERPRINT] fpcalc not available, skipping fingerprint lookup")
        return None

    # Generate fingerprint
    result = generate_fingerprint(audio_path, duration=duration)
    if not result:
        return None

    fingerprint, actual_duration = result

    # Look up in Skaldleita
    match = lookup_fingerprint(fingerprint, api_key=api_key)

    if match:
        # Add the fingerprint to the result for potential contribution
        match['_fingerprint'] = fingerprint
        match['_fingerprint_duration'] = actual_duration
        match['_source'] = 'fingerprint'
        return match

    # No match - return fingerprint for later contribution
    return {
        '_fingerprint': fingerprint,
        '_fingerprint_duration': actual_duration,
        '_no_match': True
    }


def contribute_after_identification(
    fingerprint_data: Dict[str, Any],
    identified_metadata: Dict[str, Any],
    api_key: Optional[str] = None
) -> bool:
    """
    Contribute fingerprint to Skaldleita after successful identification.

    Call this after Whisper/Gemini successfully identifies a book
    to build up the fingerprint database.

    Args:
        fingerprint_data: Dict from try_fingerprint_identification containing _fingerprint
        identified_metadata: Dict with author, title, narrator, series from AI identification
        api_key: Optional Skaldleita API key

    Returns:
        True if contribution successful
    """
    fingerprint = fingerprint_data.get('_fingerprint')
    duration = fingerprint_data.get('_fingerprint_duration', 120)

    if not fingerprint:
        logger.debug("[FINGERPRINT] No fingerprint to contribute")
        return False

    if not identified_metadata.get('title'):
        logger.debug("[FINGERPRINT] No title in metadata, skipping contribution")
        return False

    return contribute_fingerprint(
        fingerprint=fingerprint,
        duration=duration,
        book_metadata=identified_metadata,
        api_key=api_key
    )


# ============== NARRATOR VOICE ID ==============

# Skaldleita narrator voice endpoints
SKALDLEITA_NARRATOR_URL = f"{SKALDLEITA_BASE_URL}/api/narrator"

# Voice embedding model (loaded lazily)
_voice_model = None
_voice_inference = None


def _get_voice_model():
    """Lazily load the WeSpeaker voice embedding model."""
    global _voice_model, _voice_inference
    if _voice_model is None:
        try:
            from pyannote.audio import Model, Inference
            _voice_model = Model.from_pretrained("pyannote/wespeaker-voxceleb-resnet34-LM")
            _voice_inference = Inference(_voice_model, window="whole")
            logger.info("[NARRATOR] Voice embedding model loaded")
        except Exception as e:
            logger.warning(f"[NARRATOR] Could not load voice model: {e}")
            return None, None
    return _voice_model, _voice_inference


def extract_voice_embedding(
    audio_path: str,
    start_sec: int = 300,
    duration_sec: int = 30
) -> Optional[Any]:
    """
    Extract speaker voice embedding from audio file.

    Args:
        audio_path: Path to audio file
        start_sec: Start offset in seconds (default 5 min to skip intros)
        duration_sec: Duration to analyze

    Returns:
        numpy array of 256-dim embedding, or None on failure
    """
    import subprocess
    import tempfile

    _, inference = _get_voice_model()
    if inference is None:
        return None

    try:
        # Extract wav sample
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
            wav_path = tmp.name

        cmd = [
            'ffmpeg', '-y', '-i', audio_path,
            '-ss', str(start_sec),
            '-t', str(duration_sec),
            '-ar', '16000', '-ac', '1',
            '-loglevel', 'error',
            wav_path
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=60)

        if result.returncode != 0:
            logger.warning(f"[NARRATOR] Audio extraction failed: {result.stderr}")
            return None

        # Get embedding
        import numpy as np
        embedding = inference(wav_path)
        embedding = np.array(embedding, dtype=np.float32)

        # Cleanup
        os.unlink(wav_path)

        return embedding

    except Exception as e:
        logger.error(f"[NARRATOR] Voice extraction error: {e}")
        return None


def lookup_narrator(
    embedding: Any,
    threshold: float = 0.6,
    api_key: str = None
) -> Optional[Dict[str, Any]]:
    """
    Look up narrator by voice embedding in Skaldleita.

    Args:
        embedding: 256-dim numpy array from extract_voice_embedding()
        threshold: Minimum similarity for a match
        api_key: Optional Skaldleita API key

    Returns:
        Dict with narrator_name, confidence, contributor_count if matched
    """
    import base64

    try:
        emb_b64 = base64.b64encode(embedding.tobytes()).decode()

        headers = {}
        if api_key:
            headers['X-API-Key'] = api_key

        response = requests.get(
            f"{SKALDLEITA_NARRATOR_URL}/lookup",
            params={'embedding': emb_b64, 'threshold': threshold},
            headers=headers,
            timeout=10
        )

        if response.status_code == 429:
            # Narrator lookups are supplementary - fail fast, don't retry
            handle_rate_limit_response(response, 'bookdb')
            logger.warning("[NARRATOR] Lookup rate limited")
            return None

        if response.status_code == 200:
            data = response.json()
            if data.get('match'):
                logger.info(f"[NARRATOR] Voice match: {data.get('narrator_name')} "
                           f"(confidence: {data.get('confidence', 0):.2f})")
                return data

        return None

    except Exception as e:
        logger.error(f"[NARRATOR] Lookup error: {e}")
        return None


def contribute_narrator(
    embedding: Any,
    narrator_name: str,
    book_title: str = "",
    book_author: str = "",
    api_key: str = None
) -> bool:
    """
    Contribute narrator voice embedding to Skaldleita.

    Args:
        embedding: 256-dim numpy array
        narrator_name: Name of the narrator
        book_title: Source book title
        book_author: Source book author
        api_key: Optional Skaldleita API key

    Returns:
        True if contribution accepted
    """
    try:
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['X-API-Key'] = api_key

        payload = {
            'narrator_name': narrator_name,
            'embedding': embedding.tolist(),
            'source_book_title': book_title,
            'source_book_author': book_author
        }

        response = requests.post(
            SKALDLEITA_NARRATOR_URL,
            json=payload,
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                if data.get('is_new'):
                    logger.info(f"[NARRATOR] Contributed new voice profile: {narrator_name}")
                else:
                    logger.info(f"[NARRATOR] Confirmed voice profile: {narrator_name}")
                return True

        return False

    except Exception as e:
        logger.error(f"[NARRATOR] Contribution error: {e}")
        return False


def verify_narrator(
    audio_path: str,
    tagged_narrator: str,
    threshold: float = 0.5,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Verify if the tagged narrator matches the voice in the audio.

    This is the key function for detecting metadata errors.

    Args:
        audio_path: Path to audio file
        tagged_narrator: Narrator name from metadata tags
        threshold: Similarity threshold for voice matching
        api_key: Optional Skaldleita API key

    Returns:
        Dict with:
        - verified: True if voice matches tagged narrator
        - confidence: Similarity score (0-1)
        - matched_narrator: Who the voice actually matches (if different)
        - recommendation: 'correct', 'mismatch', 'unknown', 'no_profile'
    """
    result = {
        'verified': False,
        'confidence': 0.0,
        'tagged_narrator': tagged_narrator,
        'matched_narrator': None,
        'recommendation': 'unknown'
    }

    # Extract voice embedding
    embedding = extract_voice_embedding(audio_path)
    if embedding is None:
        result['recommendation'] = 'extraction_failed'
        return result

    # Look up in voice database
    match = lookup_narrator(embedding, threshold=threshold, api_key=api_key)

    if match and match.get('match'):
        matched_name = match.get('narrator_name', '')
        confidence = match.get('confidence', 0)
        result['confidence'] = confidence
        result['matched_narrator'] = matched_name

        # Check if it matches the tagged narrator (fuzzy match)
        tagged_lower = tagged_narrator.lower()
        matched_lower = matched_name.lower()

        is_match = (
            tagged_lower in matched_lower or
            matched_lower in tagged_lower or
            any(word in matched_lower for word in tagged_lower.split() if len(word) > 2)
        )

        if is_match:
            result['verified'] = True
            result['recommendation'] = 'correct'
            logger.info(f"[NARRATOR] Verified: {tagged_narrator} ✓")
        else:
            result['recommendation'] = 'mismatch'
            logger.warning(f"[NARRATOR] MISMATCH: Tagged '{tagged_narrator}' but voice matches '{matched_name}'")
    else:
        # No voice match found
        result['recommendation'] = 'no_profile'
        logger.debug(f"[NARRATOR] No voice profile for: {tagged_narrator}")

        # Still contribute this voice for future matching
        if tagged_narrator:
            contribute_narrator(
                embedding, tagged_narrator,
                book_title=os.path.basename(os.path.dirname(audio_path)),
                api_key=api_key
            )

    return result


def identify_narrator_by_voice(
    audio_path: str,
    threshold: float = 0.5,
    api_key: str = None
) -> Optional[str]:
    """
    Identify narrator purely from voice, ignoring any metadata.

    Args:
        audio_path: Path to audio file
        threshold: Minimum confidence for a match
        api_key: Optional Skaldleita API key

    Returns:
        Narrator name if confidently identified, None otherwise
    """
    embedding = extract_voice_embedding(audio_path)
    if embedding is None:
        return None

    match = lookup_narrator(embedding, threshold=threshold, api_key=api_key)

    if match and match.get('match') and match.get('confidence', 0) >= threshold:
        return match.get('narrator_name')

    return None


# Skaldleita voice storage endpoint
SKALDLEITA_VOICE_URL = f"{SKALDLEITA_BASE_URL}/api/voice"


def store_voice_signature(
    audio_path: str,
    book_title: str,
    book_author: str,
    narrator_name: str = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Store a voice signature for this audiobook.

    This stores EVERY voice, even if narrator is unknown.
    Skaldleita will cluster matching voices together with TBD_xxx IDs.
    When the narrator is eventually identified, all voices in the cluster get updated.

    Args:
        audio_path: Path to audio file
        book_title: Title of the book
        book_author: Author of the book
        narrator_name: Narrator name if known (optional)
        api_key: Optional Skaldleita API key

    Returns:
        Dict with success status, cluster_id, matched_narrator if found
    """
    # Extract voice embedding
    embedding = extract_voice_embedding(audio_path)
    if embedding is None:
        return {'success': False, 'error': 'Could not extract voice embedding'}

    try:
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['X-API-Key'] = api_key

        payload = {
            'embedding': embedding.tolist(),
            'book_title': book_title,
            'book_author': book_author,
            'narrator_name': narrator_name
        }

        response = requests.post(
            SKALDLEITA_VOICE_URL,
            json=payload,
            headers=headers,
            timeout=15
        )

        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                if data.get('matched_narrator'):
                    logger.info(f"[VOICE] Stored and matched to {data['matched_narrator']} "
                               f"({data.get('match_confidence', 0):.2f})")
                else:
                    logger.info(f"[VOICE] Stored voice for {book_author} - {book_title} "
                               f"({data.get('message', '')})")
                return data

        logger.warning(f"[VOICE] Storage failed: {response.status_code}")
        return {'success': False, 'error': f'API returned {response.status_code}'}

    except Exception as e:
        logger.error(f"[VOICE] Storage error: {e}")
        return {'success': False, 'error': str(e)}


def store_voice_after_identification(
    audio_path: str,
    result: Dict[str, Any],
    api_key: str = None
) -> bool:
    """
    Store voice signature after successful book identification.

    Call this after getting results from any audio provider.
    Always stores the voice, whether narrator is known or not.

    Args:
        audio_path: Path to audio file
        result: Identification result with author, title, narrator
        api_key: Optional Skaldleita API key

    Returns:
        True if stored successfully
    """
    if not result:
        return False

    book_title = result.get('title', '')
    book_author = result.get('author', '')
    narrator = result.get('narrator', '')

    if not book_title and not book_author:
        return False

    response = store_voice_signature(
        audio_path,
        book_title=book_title,
        book_author=book_author,
        narrator_name=narrator if narrator else None,
        api_key=api_key
    )

    return response.get('success', False)
