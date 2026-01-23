#!/usr/bin/env python3
"""
Generate a TRULY CHAOTIC test library using REAL audiobook files.

CRITICAL: Uses REAL audio files from the source library - NOT silent dummies.
This creates the kind of library that would make any sane person give up:
- Random hash filenames
- Stripped folder structure (everything flat)
- Wrong/lying metadata
- Narrator listed as author
- Partial names, abbreviations
- Non-ASCII characters mixed in
- Multiple books in one folder
- Series info missing or wrong
- Reversed author/title
- YouTube rip garbage
- Torrent naming conventions
- OCR-style typos

The goal: if Library Manager can fix THIS, it can fix anything.

REQUIREMENTS:
- Source library with real audiobooks
- Enough disk space for copies (or use hardlinks)
- Target: 500 files minimum for comprehensive testing
"""

import os
import random
import string
import shutil
from pathlib import Path
from typing import List, Tuple, Optional
import json
import subprocess

# Configuration
# IMPORTANT: Chaos library lives permanently on 4TB drive for testing
# See CLAUDE.md for documentation
SOURCE_LIBRARY = "/mnt/torrent-downloads/audiobooks"
TARGET_LIBRARY = "/mnt/4tb-storage/library-manager-chaos-test/chaos-library"
TARGET_FILE_COUNT = 500
USE_HARDLINKS = False  # Use real copies - this is permanent test data

AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus'}

def random_hash(length=8):
    """Generate a random hash-like string."""
    return ''.join(random.choices('0123456789abcdef', k=length))

def random_garbage(length=6):
    """Generate random garbage characters."""
    chars = string.ascii_letters + string.digits + '_-.()'
    return ''.join(random.choices(chars, k=length))

def mangle_name(name, level='medium'):
    """Mangle a name to various degrees of unrecognizability."""
    if not name:
        return random_garbage(8)

    if level == 'light':
        manglers = [
            lambda s: s.lower(),
            lambda s: s.upper(),
            lambda s: s.replace(' ', '_'),
            lambda s: s.replace(' ', '.'),
            lambda s: s.replace("'", ''),
            lambda s: s + f" ({random.randint(2010, 2024)})",
        ]
    elif level == 'medium':
        manglers = [
            lambda s: s.split()[0] if ' ' in s else s,
            lambda s: ''.join(w[0] for w in s.split()) if ' ' in s else s,
            lambda s: s.replace(' ', '').lower(),
            lambda s: s[:len(s)//2] + '...',
            lambda s: f"[{random_garbage(4)}] {s}",
            lambda s: f"{s} [{random_garbage(6)}]",
            lambda s: s.replace('The ', '').replace('A ', ''),
        ]
    else:  # heavy
        manglers = [
            lambda s: random_hash(12),
            lambda s: f"audiobook_{random.randint(1000, 9999)}",
            lambda s: f"book{random.randint(1, 99):02d}",
            lambda s: random_garbage(15),
            lambda s: f"download_{random_hash(8)}",
        ]

    return random.choice(manglers)(name)

def find_all_audiobooks(source_path: Path) -> List[Tuple[Path, str, str]]:
    """
    Find all audiobook files in the source library.
    Returns: List of (file_path, author_guess, title_guess) tuples
    """
    audiobooks = []

    for root, dirs, files in os.walk(source_path):
        root_path = Path(root)
        relative = root_path.relative_to(source_path)
        parts = relative.parts

        # Try to extract author/title from folder structure
        author = parts[0] if len(parts) > 0 else "Unknown"
        title = parts[1] if len(parts) > 1 else (parts[0] if len(parts) > 0 else "Unknown")

        for file in files:
            if Path(file).suffix.lower() in AUDIO_EXTENSIONS:
                file_path = root_path / file
                audiobooks.append((file_path, author, title))

    return audiobooks

def generate_chaos_filename(original_name: str, author: str, title: str, chaos_type: str) -> str:
    """Generate a chaotic filename based on the chaos type."""
    base = Path(original_name).stem
    ext = Path(original_name).suffix

    if chaos_type == 'hash_filename':
        return f"{random_hash(16)}{ext}"

    elif chaos_type == 'narrator_as_author':
        narrators = ['Ray Porter', 'Michael Kramer', 'Steven Pacey', 'Tim Gerard Reynolds', 'R.C. Bray']
        return f"{random.choice(narrators)} - {title}{ext}"

    elif chaos_type == 'reversed':
        return f"{title} - {author}{ext}"

    elif chaos_type == 'youtube_rip':
        suffixes = ['FULL AUDIOBOOK', 'Complete', 'Unabridged', 'FREE', 'HD Audio']
        quality = ['(HQ)', '(320kbps)', '[Official]', '']
        return f"{title} {random.choice(suffixes)} {random.choice(quality)}{ext}".strip()

    elif chaos_type == 'torrent_style':
        group = random.choice(['AUDIOBOOK', 'ABB', 'TAoE', 'BTN', 'YIFY'])
        year = random.randint(2015, 2024)
        return f"{author.replace(' ', '.')}.-.{title.replace(' ', '.')}.{year}.MP3.{group}{ext}"

    elif chaos_type == 'numbered_only':
        return f"{random.randint(1, 99):02d}{ext}"

    elif chaos_type == 'partial_info':
        if random.random() > 0.5:
            return f"{mangle_name(title, 'medium')}{ext}"
        else:
            return f"{mangle_name(author, 'medium')}{ext}"

    elif chaos_type == 'flat_numbered':
        prefix = random.choice(['', f'{random.randint(1, 99):02d} - ', f'Track{random.randint(1, 99):02d}_'])
        return f"{prefix}{title.replace(' ', '_')}{ext}"

    elif chaos_type == 'series_mess':
        wrong_num = random.randint(1, 20)
        fake_series = random.choice(['Saga', 'Chronicles', 'Tales', 'Adventures', 'Book'])
        patterns = [
            f"{fake_series} Book {wrong_num} - {title}{ext}",
            f"{title} ({fake_series} #{wrong_num}){ext}",
            f"[{fake_series}] {wrong_num:02d} {title}{ext}",
        ]
        return random.choice(patterns)

    elif chaos_type == 'foreign_chars':
        replacements = {'a': 'а', 'e': 'е', 'o': 'о', 'The ': 'Тhе '}  # Cyrillic lookalikes
        mangled = f"{author} - {title}"
        for orig, repl in replacements.items():
            if random.random() > 0.7:
                mangled = mangled.replace(orig, repl)
        return f"{mangled}{ext}"

    else:  # Default/mixed
        return f"{mangle_name(author, 'light')} - {mangle_name(title, 'medium')}{ext}"

def generate_chaos_folder(chaos_type: str, author: str, title: str) -> str:
    """Generate a chaotic folder path based on the chaos type."""
    if chaos_type in ['hash_filename', 'flat_numbered']:
        # Flat structure - just in root or garbage folder
        if random.random() > 0.5:
            return ""
        return f"folder_{random.randint(1, 100)}"

    elif chaos_type == 'nested_garbage':
        depth = random.randint(2, 4)
        parts = []
        for _ in range(depth):
            parts.append(random.choice([
                random_garbage(8),
                f"folder_{random.randint(1, 99)}",
                'audiobooks',
                'downloads',
                'new',
                'unsorted',
            ]))
        return '/'.join(parts)

    elif chaos_type == 'narrator_as_author':
        narrators = ['Ray Porter', 'Michael Kramer', 'Steven Pacey', 'RC Bray']
        return f"{random.choice(narrators)}/{mangle_name(title, 'light')}"

    elif chaos_type == 'reversed':
        return f"{mangle_name(title, 'light')}/{mangle_name(author, 'light')}"

    elif chaos_type == 'series_mess':
        fake_series = random.choice(['Saga', 'Chronicles', 'Tales', 'Complete Series'])
        return f"{mangle_name(author, 'light')}/{fake_series}"

    else:
        # Somewhat normal but mangled
        return f"{mangle_name(author, random.choice(['light', 'medium']))}/{mangle_name(title, random.choice(['light', 'medium']))}"

def copy_or_link_file(source: Path, target: Path, use_hardlink: bool = True):
    """Copy or hardlink a file to the target location."""
    target.parent.mkdir(parents=True, exist_ok=True)

    if use_hardlink:
        try:
            # Try hardlink first (saves space)
            target.hardlink_to(source)
        except (OSError, PermissionError):
            # Fall back to copy if hardlink fails (cross-device, permissions, etc.)
            shutil.copy2(source, target)
    else:
        shutil.copy2(source, target)

def generate_chaos_library(source_path: Path, target_path: Path, target_count: int = 500, use_hardlinks: bool = True):
    """Generate the chaos library."""

    # Clear existing chaos library (robust deletion)
    if target_path.exists():
        print(f"Clearing existing chaos library at {target_path}...")
        import subprocess
        # Use rm -rf for robust deletion (handles hardlinks better)
        subprocess.run(['rm', '-rf', str(target_path)], check=True)
    target_path.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("GENERATING CHAOS LIBRARY WITH REAL AUDIOBOOKS")
    print("=" * 60)
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print(f"Target count: {target_count}")
    print(f"Using hardlinks: {use_hardlinks}")
    print()

    # Find all audiobooks
    print("Scanning source library...")
    audiobooks = find_all_audiobooks(source_path)
    print(f"Found {len(audiobooks)} audio files")

    if len(audiobooks) == 0:
        print("ERROR: No audiobooks found in source library!")
        return

    chaos_types = [
        'hash_filename',
        'narrator_as_author',
        'reversed',
        'youtube_rip',
        'torrent_style',
        'numbered_only',
        'partial_info',
        'nested_garbage',
        'flat_numbered',
        'series_mess',
        'foreign_chars',
        'mixed',
    ]

    # Create scenarios - randomly assign chaos types to files
    created_files = []
    chaos_stats = {}

    # Ensure we hit the target count by cycling through files if needed
    file_index = 0

    print(f"\nCreating {target_count} chaotic files...\n")

    for i in range(target_count):
        # Get source file (cycle through if needed)
        source_file, author, title = audiobooks[file_index % len(audiobooks)]
        file_index += 1

        # Random chaos type
        chaos_type = random.choice(chaos_types)

        # Generate chaotic folder and filename
        folder = generate_chaos_folder(chaos_type, author, title)
        filename = generate_chaos_filename(source_file.name, author, title, chaos_type)

        # Create target path
        if folder:
            target_file = target_path / folder / filename
        else:
            target_file = target_path / filename

        # Handle filename collisions
        collision_count = 0
        original_target = target_file
        while target_file.exists():
            collision_count += 1
            stem = original_target.stem
            suffix = original_target.suffix
            target_file = original_target.parent / f"{stem}_{collision_count}{suffix}"

        # Copy or link the file
        try:
            copy_or_link_file(source_file, target_file, use_hardlinks)
            created_files.append((target_file, source_file, author, title, chaos_type))
            chaos_stats[chaos_type] = chaos_stats.get(chaos_type, 0) + 1

            if (i + 1) % 50 == 0:
                print(f"  Created {i + 1} files...")

        except Exception as e:
            print(f"  Warning: Failed to create {target_file}: {e}")

    print()
    print("=" * 60)
    print("CHAOS LIBRARY SUMMARY")
    print("=" * 60)
    print(f"\nTotal files created: {len(created_files)}")
    print(f"Location: {target_path}")
    print("\nChaos types used:")
    for chaos_type, count in sorted(chaos_stats.items(), key=lambda x: -x[1]):
        print(f"  {chaos_type:20s}: {count}")

    # Save manifest for later verification
    manifest = []
    for target_file, source_file, author, title, chaos_type in created_files:
        manifest.append({
            'chaos_file': str(target_file.relative_to(target_path)),
            'source_file': str(source_file),
            'expected_author': author,
            'expected_title': title,
            'chaos_type': chaos_type,
        })

    manifest_path = target_path / '_MANIFEST.json'
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    print(f"\nManifest saved to: {manifest_path}")

    print("\n" + "=" * 60)
    print("CHALLENGE: Can Library Manager identify all of these?")
    print("=" * 60)

    # Calculate disk usage
    if use_hardlinks:
        print("\nNote: Using hardlinks - no extra disk space used!")
    else:
        total_size = sum(f.stat().st_size for f, _, _, _, _ in created_files if f.exists())
        print(f"\nTotal disk space used: {total_size / (1024*1024*1024):.2f} GB")

def main():
    source_path = Path(SOURCE_LIBRARY)
    target_path = Path(TARGET_LIBRARY)

    if not source_path.exists():
        print(f"ERROR: Source library not found: {source_path}")
        return

    generate_chaos_library(
        source_path=source_path,
        target_path=target_path,
        target_count=TARGET_FILE_COUNT,
        use_hardlinks=USE_HARDLINKS
    )

if __name__ == '__main__':
    main()
