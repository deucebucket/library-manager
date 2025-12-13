#!/usr/bin/env python3
"""
Test script for the audio tagging/metadata embedding feature.
Creates sample audio files, runs embedding, and verifies results.

Requires: ffmpeg (for creating test audio files), mutagen

Usage:
    python test-env/test-audio-tagging.py
"""

import os
import sys
import json
import tempfile
import subprocess
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from audio_tagging import (
    collect_audio_files,
    snapshot_tags,
    write_sidecar_backup,
    embed_tags,
    embed_tags_for_path,
    build_metadata_for_embedding,
    SIDECAR_FILENAME
)


def create_silent_mp3(filepath, duration_seconds=1):
    """Create a silent MP3 file using ffmpeg."""
    try:
        subprocess.run([
            'ffmpeg', '-y', '-f', 'lavfi', '-i', f'anullsrc=r=44100:cl=mono',
            '-t', str(duration_seconds), '-q:a', '9', str(filepath)
        ], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"  Warning: Could not create MP3 (ffmpeg required): {e}")
        return False


def add_existing_tags_mp3(filepath, title=None, artist=None, album=None):
    """Add some existing tags to an MP3 file."""
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TALB

    audio = MP3(str(filepath))
    if audio.tags is None:
        audio.add_tags()
    if title:
        audio.tags['TIT2'] = TIT2(encoding=3, text=[title])
    if artist:
        audio.tags['TPE1'] = TPE1(encoding=3, text=[artist])
    if album:
        audio.tags['TALB'] = TALB(encoding=3, text=[album])
    audio.save()


def verify_tags_mp3(filepath, expected):
    """Verify MP3 tags match expected values."""
    from mutagen.mp3 import MP3

    audio = MP3(str(filepath))
    if audio.tags is None:
        return False, "No tags found"

    errors = []
    
    # Check standard tags
    tag_mapping = {
        'album': 'TALB',
        'artist': 'TPE1',
        'albumartist': 'TPE2',
        'year': 'TDRC',
    }
    
    for key, frame_id in tag_mapping.items():
        if key in expected:
            if frame_id not in audio.tags:
                errors.append(f"Missing {frame_id}")
            else:
                actual = str(audio.tags[frame_id].text[0])
                if actual != str(expected[key]):
                    errors.append(f"{frame_id}: expected '{expected[key]}', got '{actual}'")
    
    # Check custom TXXX tags
    custom_mapping = {
        'series': 'SERIES',
        'series_num': 'SERIESNUMBER',
        'narrator': 'NARRATOR',
    }
    
    for key, desc in custom_mapping.items():
        if key in expected:
            txxx_key = f'TXXX:{desc}'
            if txxx_key not in audio.tags:
                errors.append(f"Missing {txxx_key}")
            else:
                actual = str(audio.tags[txxx_key].text[0])
                if actual != str(expected[key]):
                    errors.append(f"{txxx_key}: expected '{expected[key]}', got '{actual}'")
    
    if errors:
        return False, "; ".join(errors)
    return True, "OK"


def test_collect_audio_files(test_dir):
    """Test collect_audio_files function."""
    print("\n=== Test: collect_audio_files ===")
    
    # Create test files
    (test_dir / "test1.mp3").touch()
    (test_dir / "test2.MP3").touch()  # uppercase
    (test_dir / "subdir").mkdir(exist_ok=True)
    (test_dir / "subdir" / "nested.mp3").touch()
    (test_dir / "not_audio.txt").touch()
    
    files = collect_audio_files(test_dir)
    mp3_count = len([f for f in files if f.suffix.lower() == '.mp3'])
    
    if mp3_count >= 2:  # At least test1.mp3 and nested.mp3 (uppercase might be same on case-insensitive FS)
        print(f"  PASS: Found {mp3_count} MP3 files")
        return True
    else:
        print(f"  FAIL: Expected at least 2 MP3 files, found {mp3_count}")
        return False


def test_snapshot_and_backup(test_dir):
    """Test snapshot_tags and write_sidecar_backup functions."""
    print("\n=== Test: snapshot_tags + write_sidecar_backup ===")
    
    test_file = test_dir / "snapshot_test.mp3"
    if not create_silent_mp3(test_file):
        print("  SKIP: ffmpeg not available")
        return None
    
    # Add some tags
    add_existing_tags_mp3(test_file, title="Test Title", artist="Test Artist", album="Test Album")
    
    # Snapshot
    snapshot = snapshot_tags(test_file)
    if not snapshot:
        print("  FAIL: snapshot_tags returned None")
        return False
    
    if 'tags' not in snapshot or 'file' not in snapshot:
        print(f"  FAIL: Invalid snapshot structure: {snapshot.keys()}")
        return False
    
    print(f"  Snapshot: {snapshot['tags']}")
    
    # Write sidecar
    success = write_sidecar_backup(test_dir, [snapshot])
    if not success:
        print("  FAIL: write_sidecar_backup returned False")
        return False
    
    sidecar_path = test_dir / SIDECAR_FILENAME
    if not sidecar_path.exists():
        print(f"  FAIL: Sidecar file not created at {sidecar_path}")
        return False
    
    # Verify sidecar content
    with open(sidecar_path) as f:
        sidecar_data = json.load(f)
    
    if 'files' not in sidecar_data or 'snapshot_test.mp3' not in sidecar_data['files']:
        print(f"  FAIL: Sidecar missing expected file entry")
        return False
    
    print(f"  PASS: Sidecar created with {len(sidecar_data['files'])} file(s)")
    return True


def test_embed_tags_mp3(test_dir):
    """Test embedding tags into MP3 file."""
    print("\n=== Test: embed_tags (MP3) ===")
    
    test_file = test_dir / "embed_test.mp3"
    if not create_silent_mp3(test_file):
        print("  SKIP: ffmpeg not available")
        return None
    
    # Build metadata
    metadata = build_metadata_for_embedding(
        author="Brandon Sanderson",
        title="The Final Empire",
        series="Mistborn",
        series_num="1",
        narrator="Michael Kramer",
        year="2006"
    )
    
    # Embed
    success = embed_tags(test_file, metadata, overwrite=True)
    if not success:
        print("  FAIL: embed_tags returned False")
        return False
    
    # Verify
    expected = {
        'album': "The Final Empire",
        'artist': "Brandon Sanderson",
        'albumartist': "Brandon Sanderson",
        'year': "2006",
        'series': "Mistborn",
        'series_num': "1",
        'narrator': "Michael Kramer"
    }
    
    ok, msg = verify_tags_mp3(test_file, expected)
    if ok:
        print(f"  PASS: All tags verified correctly")
        return True
    else:
        print(f"  FAIL: {msg}")
        return False


def test_embed_tags_overwrite_mode(test_dir):
    """Test that overwrite mode works correctly."""
    print("\n=== Test: embed_tags overwrite mode ===")
    
    test_file = test_dir / "overwrite_test.mp3"
    if not create_silent_mp3(test_file):
        print("  SKIP: ffmpeg not available")
        return None
    
    # Add existing tags
    add_existing_tags_mp3(test_file, title="Old Title", artist="Old Artist", album="Old Album")
    
    # Embed with new metadata (overwrite=True)
    metadata = build_metadata_for_embedding(
        author="New Author",
        title="New Title"
    )
    
    success = embed_tags(test_file, metadata, overwrite=True)
    if not success:
        print("  FAIL: embed_tags returned False")
        return False
    
    # Verify new values
    expected = {
        'album': "New Title",
        'artist': "New Author"
    }
    
    ok, msg = verify_tags_mp3(test_file, expected)
    if ok:
        print(f"  PASS: Tags overwritten correctly")
        return True
    else:
        print(f"  FAIL: {msg}")
        return False


def test_embed_tags_for_path(test_dir):
    """Test the high-level embed_tags_for_path function."""
    print("\n=== Test: embed_tags_for_path ===")
    
    book_dir = test_dir / "test_book"
    book_dir.mkdir(exist_ok=True)
    
    # Create multiple test files
    files_created = 0
    for i in range(3):
        test_file = book_dir / f"chapter_{i+1:02d}.mp3"
        if create_silent_mp3(test_file):
            files_created += 1
    
    if files_created == 0:
        print("  SKIP: ffmpeg not available")
        return None
    
    # Build metadata
    metadata = build_metadata_for_embedding(
        author="Test Author",
        title="Test Book",
        series="Test Series",
        series_num="1",
        narrator="Test Narrator",
        year="2024"
    )
    
    # Run embedding
    result = embed_tags_for_path(
        book_dir,
        metadata,
        create_backup=True,
        overwrite=True
    )
    
    if not result['success']:
        print(f"  FAIL: embed_tags_for_path failed: {result.get('error')}")
        return False
    
    if result['files_processed'] != files_created:
        print(f"  FAIL: Expected {files_created} files processed, got {result['files_processed']}")
        return False
    
    # Verify sidecar was created
    sidecar_path = book_dir / SIDECAR_FILENAME
    if not sidecar_path.exists():
        print("  FAIL: Sidecar backup not created")
        return False
    
    print(f"  PASS: Processed {result['files_processed']} files, sidecar created")
    return True


def run_tests():
    """Run all tests."""
    print("=" * 60)
    print("Audio Tagging Module Tests")
    print("=" * 60)
    
    # Create temp directory for tests
    with tempfile.TemporaryDirectory(prefix="audio_tag_test_") as tmpdir:
        test_dir = Path(tmpdir)
        print(f"Test directory: {test_dir}")
        
        results = []
        
        # Run tests
        results.append(("collect_audio_files", test_collect_audio_files(test_dir)))
        results.append(("snapshot + backup", test_snapshot_and_backup(test_dir)))
        results.append(("embed_tags (MP3)", test_embed_tags_mp3(test_dir)))
        results.append(("overwrite mode", test_embed_tags_overwrite_mode(test_dir)))
        results.append(("embed_tags_for_path", test_embed_tags_for_path(test_dir)))
        
        # Summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        
        passed = 0
        failed = 0
        skipped = 0
        
        for name, result in results:
            if result is True:
                status = "PASS"
                passed += 1
            elif result is False:
                status = "FAIL"
                failed += 1
            else:
                status = "SKIP"
                skipped += 1
            print(f"  {name}: {status}")
        
        print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")
        
        return failed == 0


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

