#!/usr/bin/env python3
"""
Generate a chaotic test library for the Chaos Handler feature.
Creates various edge cases: numbered files, tagged files, pattern files, etc.
Uses ffmpeg to create valid MP3 files and mutagen for ID3 tags.
"""

import os
import subprocess
from pathlib import Path

def create_silent_mp3(filepath, duration_seconds=1):
    """Create a silent MP3 file using ffmpeg."""
    subprocess.run([
        'ffmpeg', '-y', '-f', 'lavfi', '-i', f'anullsrc=r=44100:cl=mono',
        '-t', str(duration_seconds), '-q:a', '9', str(filepath)
    ], capture_output=True, check=True)

def add_id3_tags(filepath, title=None, artist=None, album=None, track=None):
    """Add ID3v2 tags using mutagen."""
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TRCK

    audio = MP3(filepath)
    if audio.tags is None:
        audio.add_tags()

    if title:
        audio.tags.add(TIT2(encoding=3, text=[title]))
    if artist:
        audio.tags.add(TPE1(encoding=3, text=[artist]))
    if album:
        audio.tags.add(TALB(encoding=3, text=[album]))
    if track:
        audio.tags.add(TRCK(encoding=3, text=[str(track)]))
    audio.save()

def create_test_file(filepath, title=None, artist=None, album=None, track=None, duration_seconds=1):
    """Create a test MP3 file with optional ID3v2 tags."""
    create_silent_mp3(filepath, duration_seconds)
    if any([title, artist, album, track]):
        add_id3_tags(filepath, title, artist, album, track)

def main():
    base_dir = Path('/home/deucebucket/library-manager/test-env/chaos-test-library')
    base_dir.mkdir(parents=True, exist_ok=True)

    # Clear existing files
    for f in base_dir.glob('*.mp3'):
        f.unlink()

    print("Creating chaos test library (using ffmpeg)...")

    # === GROUP 1: Completely unknown numbered files ===
    print("  Creating numbered sequence (01.mp3, 02.mp3, etc)...")
    for i in range(1, 6):
        create_test_file(base_dir / f'{i:02d}.mp3', duration_seconds=5)

    # === GROUP 2: Files with album tag (should group by metadata) ===
    print("  Creating Harry Potter chapters (with album tag)...")
    for i in range(1, 5):
        create_test_file(
            base_dir / f'chapter_{i:02d}.mp3',
            title=f'Chapter {i}',
            artist='J.K. Rowling',
            album="Harry Potter and the Sorcerer's Stone",
            track=str(i),
            duration_seconds=10
        )

    # === GROUP 3: Files with pattern in filename (should group by pattern) ===
    print("  Creating Mistborn chapter files (filename pattern)...")
    for i in range(1, 4):
        create_test_file(
            base_dir / f'mistborn_chapter_{i:02d}.mp3',
            duration_seconds=8
        )

    # === GROUP 4: Author-Title in filename ===
    print("  Creating author-title format files...")
    create_test_file(
        base_dir / 'Brandon Sanderson - The Way of Kings.mp3',
        duration_seconds=20
    )
    create_test_file(
        base_dir / 'Stephen King - IT.mp3',
        duration_seconds=30
    )

    # === GROUP 5: YouTube-rip style names ===
    print("  Creating YouTube-rip style files...")
    create_test_file(
        base_dir / 'The Great Gatsby Full Audiobook.mp3',
        duration_seconds=15
    )
    create_test_file(
        base_dir / 'Dune Audiobook Complete.mp3',
        duration_seconds=40
    )

    # === GROUP 6: Just the book title ===
    print("  Creating simple title files...")
    create_test_file(
        base_dir / 'Neuromancer.mp3',
        duration_seconds=18
    )

    # === GROUP 7: Expanse series files (same album tag) ===
    print("  Creating Expanse series files (with album tag)...")
    for i, title in enumerate(['Leviathan Wakes', 'Calibans War', 'Abaddons Gate'], 1):
        create_test_file(
            base_dir / f'expanse_{i:02d}_{title.lower().replace(" ", "_")}.mp3',
            title=title,
            artist='James S.A. Corey',
            album='The Expanse',
            track=str(i),
            duration_seconds=25
        )

    # Summary
    files = list(base_dir.glob('*.mp3'))
    print(f"\nCreated {len(files)} test files in {base_dir}")

    # Verify tags
    print("\nVerifying ID3 tags:")
    from mutagen.mp3 import MP3
    for f in sorted(files):
        try:
            audio = MP3(f)
            if audio.tags:
                album = audio.tags.get('TALB')
                artist = audio.tags.get('TPE1')
                if album or artist:
                    a = str(album.text[0]) if album else 'None'
                    r = str(artist.text[0]) if artist else 'None'
                    print(f"  {f.name}: album='{a}', artist='{r}'")
        except Exception as e:
            print(f"  {f.name}: error - {e}")

if __name__ == '__main__':
    main()
