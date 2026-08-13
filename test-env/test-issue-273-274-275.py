"""Focused regression tests for issues #273, #274, #275.

#273: ASIN from Audnexus match must be persisted to book_id.
#274: Per-book Audible region hint (WWWAUDIOFILE) must be embedded using
     audio-detected / API-detected language.
#275: "Detect language from audio" setting must actually be invoked and its
     result used.
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.models.book_profile import build_profile_from_sources
from library_manager.pipeline.layer_api import (
    _extract_book_id,
    _extract_detected_language,
)
from library_manager.pipeline.layer_ai_queue import (
    _build_audible_url,
    _extract_detected_language as _extract_detected_language_aiq,
)
from app import _extract_detected_language as _extract_detected_language_app


# Load ffmpeg helpers from the existing audio-tagging test without running it.
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "audio_tagging_test", Path(__file__).resolve().parent / "test-audio-tagging.py"
)
_audio_tagging_test = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_audio_tagging_test)
create_silent_mp3 = _audio_tagging_test.create_silent_mp3


def create_silent_m4b(filepath, duration_seconds=1):
    """Create a silent M4B/AAC file using ffmpeg."""
    import subprocess
    try:
        subprocess.run([
            'ffmpeg', '-y', '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=mono',
            '-t', str(duration_seconds), '-c:a', 'aac', '-b:a', '64k', str(filepath)
        ], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"  Warning: Could not create M4B (ffmpeg required): {e}")
        return False


def test_extract_detected_language_handles_fieldvalue_dict():
    """Language stored as a BookProfile FieldValue dict must be readable."""
    profile = {
        "language": {"value": "de", "confidence": 95, "sources": ["audio"]},
        "detected_language": None,
    }
    expected = "de"
    assert _extract_detected_language(profile) == expected
    assert _extract_detected_language_aiq(profile) == expected
    assert _extract_detected_language_app(profile) == expected
    print("[PASS] _extract_detected_language reads FieldValue dict")


def test_extract_detected_language_handles_plain_value():
    """Legacy/ebook plain top-level values must still work."""
    profile = {"detected_language": "fr"}
    assert _extract_detected_language(profile) == "fr"
    assert _extract_detected_language_aiq(profile) == "fr"
    assert _extract_detected_language_app(profile) == "fr"
    print("[PASS] _extract_detected_language reads plain value")


def test_build_profile_propagates_api_language_and_asin():
    """API candidate language and ASIN must reach the BookProfile."""
    api_candidates = [{
        "source": "bookdb",
        "author": "Andrzej Sapkowski",
        "title": "The Last Wish",
        "asin": "B003ZW6GR8",
        "language": "en",
    }]
    profile = build_profile_from_sources(
        api_candidates=api_candidates,
        folder_meta={},
        ai_result=None,
        audio_result=None,
        fingerprint_data=None,
    )
    assert profile.book_id == "B003ZW6GR8", f"expected B003ZW6GR8, got {profile.book_id}"
    assert profile.language.value == "en", f"expected en, got {profile.language.value}"
    assert "bookdb" in profile.language.sources
    print("[PASS] build_profile_from_sources propagates API language and ASIN")


def test_build_profile_propagates_audio_language():
    """Audio-detected language must reach the BookProfile."""
    audio_result = {"language": "de"}
    profile = build_profile_from_sources(
        api_candidates=[],
        folder_meta={},
        ai_result=None,
        audio_result=audio_result,
        fingerprint_data=None,
    )
    assert profile.language.value == "de"
    assert "audio" in profile.language.sources
    print("[PASS] build_profile_from_sources propagates audio language")


def test_build_audible_url_uses_detected_language():
    """German detected language must route to audible.de."""
    url = _build_audible_url("B003ZW6GR8", "de")
    assert "audible.de" in url, url
    assert url.endswith("/pd/B003ZW6GR8"), url
    print("[PASS] _build_audible_url routes German to audible.de")


def test_extract_book_id_prefers_asin():
    """ASIN must win over generic ids."""
    candidate = {"id": "123", "asin": "B003ZW6GR8", "book_id": "456"}
    assert _extract_book_id(candidate) == "B003ZW6GR8"
    print("[PASS] _extract_book_id prefers asin")


def test_build_audible_url_rejects_numeric_book_id():
    """Numeric database ids must not produce broken Audible URLs."""
    from library_manager.pipeline.layer_ai_queue import _build_audible_url as _build_audible_url_aiq
    assert _build_audible_url("12345") is None
    assert _build_audible_url(12345) is None
    assert _build_audible_url_aiq("12345") is None
    print("[PASS] _build_audible_url rejects numeric book ids")


def test_embed_tags_writes_wwwaudiofile():
    """WWWAUDIOFILE must be written for MP3 and MP4 containers."""
    from audio_tagging import embed_tags_mp3, embed_tags_mp4, build_metadata_for_embedding

    metadata = build_metadata_for_embedding(
        author="Test Author",
        title="Test Book",
        book_id="B003ZW6GR8",
        audible_url="https://www.audible.de/pd/B003ZW6GR8",
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # MP3 via ID3 TXXX frame
        mp3_path = tmp_path / "test.mp3"
        assert create_silent_mp3(mp3_path), "ffmpeg required for MP3 test"
        assert embed_tags_mp3(mp3_path, metadata)
        from mutagen.mp3 import MP3
        tags = MP3(str(mp3_path))
        assert "TXXX:WWWAUDIOFILE" in tags
        assert str(tags["TXXX:WWWAUDIOFILE"]) == "https://www.audible.de/pd/B003ZW6GR8"

        # MP4 via freeform atom
        m4b_path = tmp_path / "test.m4b"
        assert create_silent_m4b(m4b_path), "ffmpeg required for M4B test"
        assert embed_tags_mp4(m4b_path, metadata)
        from mutagen.mp4 import MP4
        mp4_tags = MP4(str(m4b_path))
        key = "----:com.apple.iTunes:WWWAUDIOFILE"
        assert key in mp4_tags
        assert mp4_tags[key][0].decode("utf-8") == "https://www.audible.de/pd/B003ZW6GR8"

    print("[PASS] embed_tags writes WWWAUDIOFILE for MP3 and MP4")


def main():
    test_extract_detected_language_handles_fieldvalue_dict()
    test_extract_detected_language_handles_plain_value()
    test_build_profile_propagates_api_language_and_asin()
    test_build_profile_propagates_audio_language()
    test_build_audible_url_uses_detected_language()
    test_extract_book_id_prefers_asin()
    test_build_audible_url_rejects_numeric_book_id()
    test_embed_tags_writes_wwwaudiofile()
    print("\nAll issue #273/#274/#275 regression tests passed.")


if __name__ == "__main__":
    main()
