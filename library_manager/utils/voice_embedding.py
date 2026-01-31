"""Voice embedding extraction for narrator identification.

Extracts 256-dimensional speaker embeddings using WeSpeaker ONNX model.
These embeddings can be used to identify narrators and group audiobooks
by the same narrator.

The embedding is computed locally and sent to Skaldleita, offloading
compute from the server to the client.

Uses raw ONNX Runtime (~15MB) + model file (~27MB) = ~42MB total.
NO torch, NO cuda, NO 2GB downloads.
"""

import os
import logging
import subprocess
import tempfile
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# Model configuration
MODEL_URL = "https://huggingface.co/Wespeaker/wespeaker-voxceleb-resnet34-LM/resolve/main/voxceleb_resnet34_LM.onnx"
MODEL_FILENAME = "voxceleb_resnet34_LM.onnx"
MODEL_SIZE_MB = 27  # Approximate size for progress indication

# Global model session (lazy-loaded once, reused)
_onnx_session = None
_session_lock = threading.Lock()
_model_load_attempted = False
_onnx_available = None


def _get_model_path():
    """Get path to cached model file, downloading if needed."""
    # Store in user's cache directory
    cache_dir = Path.home() / ".cache" / "library-manager" / "models"
    cache_dir.mkdir(parents=True, exist_ok=True)
    model_path = cache_dir / MODEL_FILENAME

    if model_path.exists() and model_path.stat().st_size > 1000000:  # >1MB = valid
        return model_path

    # Download model
    logger.info(f"[VOICE] Downloading WeSpeaker ONNX model (~{MODEL_SIZE_MB}MB)...")
    try:
        import urllib.request
        urllib.request.urlretrieve(MODEL_URL, model_path)
        logger.info(f"[VOICE] Model downloaded to {model_path}")
        return model_path
    except Exception as e:
        logger.warning(f"[VOICE] Failed to download model: {e}")
        return None


def is_voice_embedding_available():
    """Check if voice embedding dependencies are installed."""
    global _onnx_available
    if _onnx_available is not None:
        return _onnx_available

    try:
        import onnxruntime
        import numpy
        _onnx_available = True
    except ImportError:
        _onnx_available = False
        logger.debug("[VOICE] onnxruntime not installed - voice embedding disabled")

    return _onnx_available


def _get_onnx_session():
    """Get or create ONNX inference session."""
    global _onnx_session, _model_load_attempted

    if not is_voice_embedding_available():
        return None

    if _onnx_session is None and not _model_load_attempted:
        with _session_lock:
            if _onnx_session is None and not _model_load_attempted:
                _model_load_attempted = True
                try:
                    import onnxruntime as ort

                    model_path = _get_model_path()
                    if model_path is None:
                        return None

                    logger.info("[VOICE] Loading WeSpeaker ONNX model...")

                    # CPU-only session options
                    opts = ort.SessionOptions()
                    opts.inter_op_num_threads = 1
                    opts.intra_op_num_threads = 2

                    _onnx_session = ort.InferenceSession(
                        str(model_path),
                        sess_options=opts,
                        providers=['CPUExecutionProvider']
                    )

                    logger.info("[VOICE] WeSpeaker ONNX model loaded (256-dim, CPU)")
                except Exception as e:
                    logger.warning(f"[VOICE] Failed to load ONNX model: {e}")
                    return None

    return _onnx_session


def _compute_fbank(audio_path: str, num_mel_bins: int = 80, sample_rate: int = 16000):
    """
    Compute filter bank features from audio file.
    WeSpeaker expects 80-dim mel filterbank features.
    """
    import numpy as np

    # Use ffmpeg to get raw PCM
    with tempfile.NamedTemporaryFile(suffix='.raw', delete=False) as tmp:
        tmp_raw = tmp.name

    try:
        cmd = [
            'ffmpeg', '-y', '-i', audio_path,
            '-ar', str(sample_rate),
            '-ac', '1',
            '-f', 's16le',
            '-acodec', 'pcm_s16le',
            '-loglevel', 'error',
            tmp_raw
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0:
            return None

        # Read PCM data
        audio = np.fromfile(tmp_raw, dtype=np.int16).astype(np.float32) / 32768.0

        if len(audio) < sample_rate:  # Less than 1 second
            return None

        # Compute mel spectrogram using numpy (no librosa/torch needed)
        # Parameters matching WeSpeaker defaults
        frame_length = int(0.025 * sample_rate)  # 25ms
        frame_step = int(0.010 * sample_rate)    # 10ms
        n_fft = 512

        # Pre-emphasis
        audio = np.append(audio[0], audio[1:] - 0.97 * audio[:-1])

        # Framing
        num_frames = 1 + (len(audio) - frame_length) // frame_step
        if num_frames < 10:
            return None

        frames = np.zeros((num_frames, frame_length))
        for i in range(num_frames):
            start = i * frame_step
            frames[i] = audio[start:start + frame_length]

        # Windowing (Hamming)
        window = np.hamming(frame_length)
        frames *= window

        # FFT
        mag_spec = np.abs(np.fft.rfft(frames, n_fft))
        pow_spec = mag_spec ** 2

        # Mel filterbank
        low_freq = 20
        high_freq = sample_rate // 2

        def hz_to_mel(hz):
            return 2595 * np.log10(1 + hz / 700)

        def mel_to_hz(mel):
            return 700 * (10 ** (mel / 2595) - 1)

        mel_points = np.linspace(hz_to_mel(low_freq), hz_to_mel(high_freq), num_mel_bins + 2)
        hz_points = mel_to_hz(mel_points)
        bin_points = np.floor((n_fft + 1) * hz_points / sample_rate).astype(int)

        fbank = np.zeros((num_mel_bins, n_fft // 2 + 1))
        for i in range(num_mel_bins):
            for j in range(bin_points[i], bin_points[i + 1]):
                fbank[i, j] = (j - bin_points[i]) / (bin_points[i + 1] - bin_points[i])
            for j in range(bin_points[i + 1], bin_points[i + 2]):
                fbank[i, j] = (bin_points[i + 2] - j) / (bin_points[i + 2] - bin_points[i + 1])

        # Apply filterbank
        mel_spec = np.dot(pow_spec, fbank.T)
        mel_spec = np.where(mel_spec == 0, np.finfo(float).eps, mel_spec)
        log_mel = np.log(mel_spec)

        # Normalize (CMN - cepstral mean normalization)
        log_mel = log_mel - np.mean(log_mel, axis=0)

        return log_mel.astype(np.float32)

    finally:
        if os.path.exists(tmp_raw):
            os.unlink(tmp_raw)


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
    import numpy as np

    session = _get_onnx_session()
    if session is None:
        return None

    try:
        # Extract audio segment first
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
            tmp_wav = tmp.name

        try:
            cmd = [
                'ffmpeg', '-y', '-i', audio_path,
                '-ss', str(start_sec),
                '-t', str(duration_sec),
                '-ar', '16000',
                '-ac', '1',
                '-f', 'wav',
                '-loglevel', 'error',
                tmp_wav
            ]

            result = subprocess.run(cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                logger.debug(f"[VOICE] ffmpeg failed: {result.stderr.decode()[:200]}")
                return None

            if os.path.getsize(tmp_wav) < 1000:
                logger.debug("[VOICE] Extracted WAV too small")
                return None

            # Compute mel filterbank features
            fbank = _compute_fbank(tmp_wav)
            if fbank is None:
                logger.debug("[VOICE] Failed to compute filterbank features")
                return None

            # Prepare input for ONNX model
            # WeSpeaker expects shape: (batch, frames, mel_bins)
            fbank = np.expand_dims(fbank, axis=0)  # Add batch dimension

            # Run inference
            input_name = session.get_inputs()[0].name
            output_name = session.get_outputs()[0].name

            embedding = session.run([output_name], {input_name: fbank})[0]

            # Flatten and normalize
            embedding = embedding.flatten()
            embedding = embedding / (np.linalg.norm(embedding) + 1e-8)

            # Should be 256 dimensions
            if len(embedding) != 256:
                logger.warning(f"[VOICE] Unexpected embedding size: {len(embedding)}")
                # Truncate or pad
                if len(embedding) > 256:
                    embedding = embedding[:256]
                else:
                    embedding = np.pad(embedding, (0, 256 - len(embedding)))

            return embedding.tolist()

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
    return extract_voice_embedding(clip_path, start_sec=0, duration_sec=30)


__all__ = [
    'is_voice_embedding_available',
    'extract_voice_embedding',
    'extract_voice_embedding_from_clip',
]
