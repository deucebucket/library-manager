"""Audio file utilities for sample extraction and discovery."""
import os
import re
import glob
import subprocess
import tempfile
import logging

logger = logging.getLogger(__name__)

# File extension constants
AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus', '.wma', '.aac'}
EBOOK_EXTENSIONS = {'.epub', '.pdf', '.mobi', '.azw3'}


def get_first_audio_file(folder_path):
    """
    Get the audio file that sorts first in a folder (usually contains credits).
    Audiobook credits (title/author/narrator) are typically announced in the
    first 30-60 seconds of the first file.
    """
    # Get all audio files
    audio_extensions = ['*.mp3', '*.m4b', '*.m4a', '*.flac', '*.ogg', '*.opus']
    audio_files = []
    for ext in audio_extensions:
        audio_files.extend(glob.glob(os.path.join(folder_path, ext)))
        audio_files.extend(glob.glob(os.path.join(folder_path, ext.upper())))

    if not audio_files:
        return None

    # Sort naturally (so "2" comes before "10")
    def natural_sort_key(s):
        return [int(text) if text.isdigit() else text.lower()
                for text in re.split('([0-9]+)', os.path.basename(s))]

    audio_files.sort(key=natural_sort_key)
    return audio_files[0]


def extract_audio_sample(audio_file, duration_seconds=90, output_format='mp3'):
    """
    Extract first N seconds of audio file for analysis.
    Returns path to temp file or None on failure.
    """
    try:
        # Create temp file for the sample
        temp_file = tempfile.NamedTemporaryFile(suffix=f'.{output_format}', delete=False)
        temp_path = temp_file.name
        temp_file.close()

        # Use ffmpeg to extract sample
        cmd = [
            'ffmpeg', '-y',
            '-i', audio_file,
            '-t', str(duration_seconds),  # Duration
            '-vn',  # No video
            '-acodec', 'libmp3lame' if output_format == 'mp3' else 'aac',
            '-b:a', '64k',  # Low bitrate for smaller file
            '-ar', '16000',  # 16kHz sample rate (good for speech)
            '-ac', '1',  # Mono
            temp_path
        ]

        result = subprocess.run(cmd, capture_output=True, timeout=60)

        if result.returncode == 0 and os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            return temp_path
        else:
            logger.debug(f"Audio extraction failed: {result.stderr.decode()[:200]}")
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return None

    except subprocess.TimeoutExpired:
        logger.debug("Audio extraction timed out")
        return None
    except Exception as e:
        logger.debug(f"Audio extraction error: {e}")
        return None


def extract_audio_sample_from_middle(audio_file, duration_seconds=60, output_format='mp3'):
    """
    Extract audio from the MIDDLE of the file (for content-based identification).
    This avoids intro music/credits and gets actual story content.
    Returns path to temp file or None on failure.
    """
    try:
        # First, get the total duration of the file
        probe_cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            audio_file
        ]
        result = subprocess.run(probe_cmd, capture_output=True, timeout=30)
        if result.returncode != 0:
            return None

        total_duration = float(result.stdout.decode().strip())

        # Start at 10% into the file, or 5 minutes, whichever is less
        # This skips intros, music, and credits
        start_time = min(total_duration * 0.1, 300)  # 5 minutes max skip

        # Make sure we have enough content left
        if start_time + duration_seconds > total_duration:
            start_time = max(0, total_duration - duration_seconds - 10)

        # Create temp file for the sample
        temp_file = tempfile.NamedTemporaryFile(suffix=f'.{output_format}', delete=False)
        temp_path = temp_file.name
        temp_file.close()

        # Extract sample from middle
        cmd = [
            'ffmpeg', '-y',
            '-ss', str(start_time),  # Start position
            '-i', audio_file,
            '-t', str(duration_seconds),  # Duration
            '-vn',  # No video
            '-acodec', 'libmp3lame' if output_format == 'mp3' else 'aac',
            '-b:a', '64k',  # Low bitrate for smaller file
            '-ar', '16000',  # 16kHz sample rate
            '-ac', '1',  # Mono
            temp_path
        ]

        result = subprocess.run(cmd, capture_output=True, timeout=60)

        if result.returncode == 0 and os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            return temp_path
        else:
            logger.debug(f"Middle audio extraction failed: {result.stderr.decode()[:200]}")
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return None

    except subprocess.TimeoutExpired:
        logger.debug("Middle audio extraction timed out")
        return None
    except Exception as e:
        logger.debug(f"Middle audio extraction error: {e}")
        return None


def find_audio_files(directory):
    """Recursively find all audio files in directory."""
    audio_files = []
    for root, dirs, files in os.walk(directory, followlinks=True):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext in AUDIO_EXTENSIONS:
                audio_files.append(os.path.join(root, f))
    return audio_files


def find_ebook_files(directory):
    """Recursively find all ebook files in directory."""
    ebook_files = []
    for root, dirs, files in os.walk(directory, followlinks=True):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext in EBOOK_EXTENSIONS:
                ebook_files.append(os.path.join(root, f))
    return ebook_files


__all__ = [
    'AUDIO_EXTENSIONS',
    'EBOOK_EXTENSIONS',
    'get_first_audio_file',
    'extract_audio_sample',
    'extract_audio_sample_from_middle',
    'find_audio_files',
    'find_ebook_files',
]
