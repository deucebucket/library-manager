"""Voice embedding extraction for narrator identification.

Extracts 256-dimensional speaker embeddings using WeSpeaker (ONNX runtime).
These embeddings can be used to identify narrators and group audiobooks
by the same narrator.

The embedding is computed locally and sent to Skaldleita, offloading
compute from the server to the client.

Uses wespeakerruntime (ONNX) instead of pyannote/torch for a much smaller
footprint (~50MB vs ~2GB).
"""

import os
import logging
import subprocess
import tempfile
import threading

logger = logging.getLogger(__name__)

# Global model (lazy-loaded once, reused)
_voice_model = None
_voice_model_lock = threading.Lock()
_model_load_attempted = False
_model_available = None


def is_voice_embedding_available():
    """Check if voice embedding dependencies are installed."""
    global _model_available
    if _model_available is not None:
        return _model_available

    try:
        import wespeaker
        _model_available = True
    except ImportError:
        _model_available = False
        logger.debug("[VOICE] wespeakerruntime not installed - voice embedding disabled")

    return _model_available


def _get_voice_model():
    """Get or load the WeSpeaker voice embedding model (ONNX)."""
    global _voice_model, _model_load_attempted

    if not is_voice_embedding_available():
        return None

    if _voice_model is None and not _model_load_attempted:
        with _voice_model_lock:
            if _voice_model is None and not _model_load_attempted:
                _model_load_attempted = True
                try:
                    import wespeaker

                    logger.info("[VOICE] Loading WeSpeaker ONNX model (256-dim)...")

                    # Use English model (voxceleb trained, same as Skaldleita)
                    # This downloads ~50MB model on first use
                    _voice_model = wespeaker.load_model('english')
                    _voice_model.set_gpu(-1)  # CPU mode

                    logger.info("[VOICE] WeSpeaker ONNX model loaded successfully")
                except Exception as e:
                    logger.warning(f"[VOICE] Failed to load model: {e}")
                    return None

    return _voice_model


def extract_voice_embedding(audio_path: str, start_sec: int = 0, duration_sec: int = 30):
    """
    Extract 256-dim voice embedding from audio file.

    Args:
        audio_path: Path to audio file (MP3, M4B, etc)
        start_sec: Start position in seconds
        duration_sec: Duration to analyze (default 30 sec)

    Returns:
        List of 256 floats, or None on failure
    """
    model = _get_voice_model()
    if model is None:
        return None

    try:
        # Extract audio segment to WAV (wespeaker needs WAV)
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
            tmp_wav = tmp.name

        try:
            cmd = [
                'ffmpeg', '-y', '-i', audio_path,
                '-ss', str(start_sec),
                '-t', str(duration_sec),
                '-ar', '16000',  # 16kHz for speech models
                '-ac', '1',      # mono
                '-f', 'wav',
                '-loglevel', 'error',
                tmp_wav
            ]

            result = subprocess.run(cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                logger.debug(f"[VOICE] ffmpeg failed: {result.stderr.decode()[:200]}")
                return None

            # Check file size
            if os.path.getsize(tmp_wav) < 1000:
                logger.debug("[VOICE] Extracted WAV too small")
                return None

            # Extract embedding using WeSpeaker ONNX
            embedding = model.extract_embedding(tmp_wav)

            # Ensure we have 256 dimensions
            if hasattr(embedding, 'tolist'):
                embedding = embedding.tolist()

            if isinstance(embedding, list) and len(embedding) == 256:
                return embedding
            elif isinstance(embedding, list) and len(embedding) > 0:
                # Some models return nested list
                if isinstance(embedding[0], list):
                    embedding = embedding[0]
                if len(embedding) == 256:
                    return embedding

            logger.warning(f"[VOICE] Unexpected embedding format: {type(embedding)}, len={len(embedding) if hasattr(embedding, '__len__') else 'N/A'}")
            return None

        finally:
            if os.path.exists(tmp_wav):
                os.unlink(tmp_wav)

    except subprocess.TimeoutExpired:
        logger.debug("[VOICE] ffmpeg timeout")
        return None
    except Exception as e:
        logger.debug(f"[VOICE] Embedding extraction failed: {e}")
        return None


def extract_voice_embedding_from_clip(clip_path: str):
    """
    Extract embedding from an already-extracted audio clip.

    Use this when you already have a temporary clip file (e.g., during
    Skaldleita audio identification) to avoid re-extracting audio.

    Args:
        clip_path: Path to audio clip (MP3 or WAV)

    Returns:
        List of 256 floats, or None on failure
    """
    # For clips, analyze the first 30 seconds (or whole clip if shorter)
    return extract_voice_embedding(clip_path, start_sec=0, duration_sec=30)


__all__ = [
    'is_voice_embedding_available',
    'extract_voice_embedding',
    'extract_voice_embedding_from_clip',
]
