"""
File Validation - Check audio files before processing.

Uses ffprobe to detect corrupt, truncated, or invalid files
before wasting Skaldleita's time on garbage.
"""

import subprocess
import json
import logging
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)

# Minimum requirements for a valid audiobook
MIN_DURATION_SECONDS = 600  # 10 minutes
MIN_FILE_SIZE_BYTES = 1_000_000  # 1 MB
FFPROBE_TIMEOUT = 30  # seconds


def validate_audio_file(path: str) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Validate an audio file using ffprobe.

    Returns:
        (is_valid, reason, metadata)
        - is_valid: True if file is a valid audiobook
        - reason: "valid" or error description
        - metadata: Dict with duration, size, format info (empty if invalid)
    """
    file_path = Path(path)

    # Basic checks
    if not file_path.exists():
        return False, "file_not_found", {}

    if not file_path.is_file():
        return False, "not_a_file", {}

    file_size = file_path.stat().st_size
    if file_size < MIN_FILE_SIZE_BYTES:
        return False, "too_small", {"size": file_size}

    # Run ffprobe
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration,size,bit_rate,format_name',
                '-show_entries', 'stream=codec_type,codec_name,duration',
                '-of', 'json',
                str(file_path)
            ],
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip()[:100] if result.stderr else "ffprobe_error"
            logger.warning(f"ffprobe failed for {path}: {error_msg}")
            return False, "ffprobe_error", {"error": error_msg}

        data = json.loads(result.stdout)

    except subprocess.TimeoutExpired:
        logger.warning(f"ffprobe timeout for {path}")
        return False, "ffprobe_timeout", {}
    except json.JSONDecodeError:
        logger.warning(f"ffprobe returned invalid JSON for {path}")
        return False, "ffprobe_invalid_output", {}
    except FileNotFoundError:
        logger.error("ffprobe not installed - cannot validate files")
        return False, "ffprobe_not_installed", {}
    except Exception as e:
        logger.warning(f"Validation error for {path}: {e}")
        return False, f"validation_error", {"error": str(e)}

    # Extract metadata
    format_info = data.get("format", {})
    streams = data.get("streams", [])

    duration_str = format_info.get("duration", "0")
    try:
        duration = float(duration_str)
    except (ValueError, TypeError):
        duration = 0

    bit_rate_str = format_info.get("bit_rate", "0")
    try:
        bit_rate = int(bit_rate_str)
    except (ValueError, TypeError):
        bit_rate = 0

    format_name = format_info.get("format_name", "unknown")

    # Check for audio stream
    has_audio = any(s.get("codec_type") == "audio" for s in streams)
    audio_codec = next(
        (s.get("codec_name") for s in streams if s.get("codec_type") == "audio"),
        None
    )

    metadata = {
        "duration": duration,
        "duration_formatted": format_duration(duration),
        "size": file_size,
        "bit_rate": bit_rate,
        "format": format_name,
        "has_audio": has_audio,
        "audio_codec": audio_codec,
    }

    # Validation checks
    if not has_audio:
        return False, "no_audio_stream", metadata

    if duration == 0:
        return False, "no_duration_truncated", metadata

    if duration < MIN_DURATION_SECONDS:
        return False, "too_short", metadata

    # Try to seek to end (catches truncated files)
    if not can_seek_to_end(str(file_path)):
        return False, "truncated_cant_seek_end", metadata

    return True, "valid", metadata


def can_seek_to_end(path: str) -> bool:
    """Check if we can read the last 10 seconds of the file."""
    try:
        result = subprocess.run(
            [
                'ffmpeg', '-v', 'error',
                '-sseof', '-10',  # Seek to 10 seconds before end
                '-i', path,
                '-f', 'null', '-'
            ],
            capture_output=True,
            timeout=FFPROBE_TIMEOUT
        )
        return result.returncode == 0
    except:
        return False


def format_duration(seconds: float) -> str:
    """Format duration as HH:MM:SS."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def batch_validate(paths: list, progress_callback=None) -> Dict[str, Dict]:
    """
    Validate multiple files.

    Returns dict of {path: {"valid": bool, "reason": str, "metadata": dict}}
    """
    results = {}
    total = len(paths)

    for i, path in enumerate(paths):
        is_valid, reason, metadata = validate_audio_file(path)
        results[path] = {
            "valid": is_valid,
            "reason": reason,
            "metadata": metadata
        }

        if progress_callback:
            progress_callback(i + 1, total, path, is_valid)

    return results


def get_validation_summary(results: Dict[str, Dict]) -> Dict[str, Any]:
    """Get summary statistics from batch validation results."""
    total = len(results)
    valid = sum(1 for r in results.values() if r["valid"])
    invalid = total - valid

    # Group by reason
    reasons = {}
    for r in results.values():
        reason = r["reason"]
        reasons[reason] = reasons.get(reason, 0) + 1

    return {
        "total": total,
        "valid": valid,
        "invalid": invalid,
        "by_reason": reasons
    }


# Export public API
__all__ = [
    'validate_audio_file',
    'batch_validate',
    'get_validation_summary',
    'format_duration',
    'MIN_DURATION_SECONDS',
    'MIN_FILE_SIZE_BYTES',
]
