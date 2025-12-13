"""
Audio Tagging Module for Library Manager

Handles embedding audiobook metadata into audio files using mutagen.
Supports MP3, M4B/M4A/AAC, FLAC, Ogg/Opus, and WMA formats.
Creates sidecar JSON backups of original tags before modification.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Audio extensions we can potentially tag
TAGGABLE_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus', '.wma', '.aac'}

# Sidecar backup filename
SIDECAR_FILENAME = '.library-manager.tags.json'


def collect_audio_files(target_path: Path) -> List[Path]:
    """
    Collect all taggable audio files from a path.
    If target_path is a file, returns [target_path] if it's audio.
    If target_path is a directory, walks it recursively.
    """
    target = Path(target_path)
    audio_files = []

    if target.is_file():
        if target.suffix.lower() in TAGGABLE_EXTENSIONS:
            audio_files.append(target)
    elif target.is_dir():
        for ext in TAGGABLE_EXTENSIONS:
            audio_files.extend(target.rglob(f'*{ext}'))
            # Also check uppercase
            audio_files.extend(target.rglob(f'*{ext.upper()}'))
    
    # Deduplicate (in case of case-insensitive filesystem)
    seen = set()
    unique_files = []
    for f in audio_files:
        resolved = f.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique_files.append(f)
    
    return sorted(unique_files)


def snapshot_tags(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Read existing tags from an audio file and return a JSON-safe representation.
    Skips binary data like cover art.
    Returns None if file cannot be read.
    """
    try:
        from mutagen import File
        from mutagen.mp3 import MP3
        from mutagen.mp4 import MP4
        from mutagen.flac import FLAC
        from mutagen.oggvorbis import OggVorbis
        from mutagen.oggopus import OggOpus
        from mutagen.asf import ASF

        audio = File(str(file_path))
        if audio is None:
            return None

        snapshot = {
            'file': str(file_path.name),
            'format': type(audio).__name__,
            'timestamp': datetime.now().isoformat(),
            'tags': {}
        }

        if isinstance(audio, MP3) and audio.tags:
            # ID3 tags - extract text values only
            for key, value in audio.tags.items():
                if hasattr(value, 'text'):
                    # Text frames (TIT2, TPE1, etc.)
                    snapshot['tags'][key] = [str(t) for t in value.text]
                elif key.startswith('TXXX:'):
                    # User-defined text frames
                    snapshot['tags'][key] = [str(t) for t in value.text] if hasattr(value, 'text') else str(value)
                # Skip binary frames like APIC (cover art)

        elif isinstance(audio, MP4) and audio.tags:
            # MP4/M4B/M4A tags
            for key, value in audio.tags.items():
                if key.startswith('covr'):
                    continue  # Skip cover art
                if isinstance(value, list):
                    snapshot['tags'][key] = [str(v) if not isinstance(v, bytes) else None for v in value]
                    snapshot['tags'][key] = [v for v in snapshot['tags'][key] if v is not None]
                else:
                    if not isinstance(value, bytes):
                        snapshot['tags'][key] = str(value)

        elif isinstance(audio, (FLAC, OggVorbis, OggOpus)) and audio.tags:
            # Vorbis comments
            for key, value in audio.tags.items():
                if key.lower() in ('metadata_block_picture', 'coverart'):
                    continue  # Skip cover art
                snapshot['tags'][key] = list(value) if isinstance(value, list) else [str(value)]

        elif isinstance(audio, ASF) and audio.tags:
            # WMA/ASF tags
            for key, value in audio.tags.items():
                if 'picture' in key.lower():
                    continue  # Skip pictures
                if hasattr(value, 'value'):
                    snapshot['tags'][key] = str(value.value)
                else:
                    snapshot['tags'][key] = str(value)

        return snapshot

    except Exception as e:
        logger.debug(f"Could not snapshot tags from {file_path}: {e}")
        return None


def write_sidecar_backup(folder: Path, snapshots: List[Dict[str, Any]]) -> bool:
    """
    Write/update a sidecar JSON backup file with tag snapshots.
    If the sidecar already exists, merge new snapshots (update by filename).
    Returns True on success.
    """
    sidecar_path = Path(folder) / SIDECAR_FILENAME

    existing_data = {'version': 1, 'created': datetime.now().isoformat(), 'files': {}}

    # Load existing sidecar if present
    if sidecar_path.exists():
        try:
            with open(sidecar_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception as e:
            logger.warning(f"Could not read existing sidecar {sidecar_path}: {e}")

    # Update with new snapshots
    for snap in snapshots:
        if snap and 'file' in snap:
            filename = snap['file']
            # Keep the first backup (original), don't overwrite
            if filename not in existing_data.get('files', {}):
                existing_data.setdefault('files', {})[filename] = snap

    # Ensure updated timestamp
    existing_data['updated'] = datetime.now().isoformat()

    try:
        with open(sidecar_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)
        logger.debug(f"Wrote sidecar backup to {sidecar_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write sidecar backup {sidecar_path}: {e}")
        return False


def embed_tags_mp3(file_path: Path, metadata: Dict[str, Any], overwrite: bool = True) -> bool:
    """Embed tags into MP3 file using ID3v2."""
    try:
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3, TIT2, TALB, TPE1, TPE2, TDRC, TXXX

        audio = MP3(str(file_path))

        # Create ID3 tag if none exists
        if audio.tags is None:
            audio.add_tags()

        tags = audio.tags

        # Standard tags
        tag_mapping = [
            ('title', TIT2, 'TIT2'),       # Track/chapter title -> book title for albums
            ('album', TALB, 'TALB'),       # Album -> book title
            ('artist', TPE1, 'TPE1'),      # Artist -> author
            ('albumartist', TPE2, 'TPE2'), # Album artist -> author
        ]

        for meta_key, frame_class, frame_id in tag_mapping:
            value = metadata.get(meta_key)
            if value:
                if overwrite or frame_id not in tags:
                    tags[frame_id] = frame_class(encoding=3, text=[str(value)])

        # Year
        year = metadata.get('year')
        if year:
            if overwrite or 'TDRC' not in tags:
                tags['TDRC'] = TDRC(encoding=3, text=[str(year)])

        # Custom tags via TXXX frames
        custom_tags = [
            ('series', 'SERIES'),
            ('series_num', 'SERIESNUMBER'),
            ('narrator', 'NARRATOR'),
            ('edition', 'EDITION'),
            ('variant', 'VARIANT'),
        ]

        for meta_key, txxx_desc in custom_tags:
            value = metadata.get(meta_key)
            if value:
                txxx_key = f'TXXX:{txxx_desc}'
                if overwrite or txxx_key not in tags:
                    tags[txxx_key] = TXXX(encoding=3, desc=txxx_desc, text=[str(value)])

        audio.save()
        return True

    except Exception as e:
        logger.error(f"Failed to embed MP3 tags in {file_path}: {e}")
        return False


def embed_tags_mp4(file_path: Path, metadata: Dict[str, Any], overwrite: bool = True) -> bool:
    """Embed tags into MP4/M4B/M4A/AAC file."""
    try:
        from mutagen.mp4 import MP4

        audio = MP4(str(file_path))

        if audio.tags is None:
            audio.add_tags()

        tags = audio.tags

        # Standard MP4 tags
        standard_tags = [
            ('title', '\xa9nam'),      # Title
            ('album', '\xa9alb'),      # Album
            ('artist', '\xa9ART'),     # Artist
            ('albumartist', 'aART'),   # Album artist
        ]

        for meta_key, mp4_key in standard_tags:
            value = metadata.get(meta_key)
            if value:
                if overwrite or mp4_key not in tags:
                    tags[mp4_key] = [str(value)]

        # Year
        year = metadata.get('year')
        if year:
            if overwrite or '\xa9day' not in tags:
                tags['\xa9day'] = [str(year)]

        # Custom tags via freeform atoms (iTunes style)
        custom_tags = [
            ('series', 'SERIES'),
            ('series_num', 'SERIESNUMBER'),
            ('narrator', 'NARRATOR'),
            ('edition', 'EDITION'),
            ('variant', 'VARIANT'),
        ]

        for meta_key, tag_name in custom_tags:
            value = metadata.get(meta_key)
            if value:
                # Use ----:com.apple.iTunes:TAGNAME format
                freeform_key = f'----:com.apple.iTunes:{tag_name}'
                if overwrite or freeform_key not in tags:
                    # MP4FreeForm expects bytes
                    tags[freeform_key] = [str(value).encode('utf-8')]

        audio.save()
        return True

    except Exception as e:
        logger.error(f"Failed to embed MP4 tags in {file_path}: {e}")
        return False


def embed_tags_vorbis(file_path: Path, metadata: Dict[str, Any], overwrite: bool = True) -> bool:
    """Embed tags into FLAC/Ogg/Opus files using Vorbis comments."""
    try:
        from mutagen import File

        audio = File(str(file_path))
        if audio is None:
            return False

        if audio.tags is None:
            # Different file types have different ways to add tags
            if hasattr(audio, 'add_tags'):
                audio.add_tags()
            else:
                return False

        tags = audio.tags

        # Standard Vorbis comments (uppercase by convention)
        standard_tags = [
            ('title', 'TITLE'),
            ('album', 'ALBUM'),
            ('artist', 'ARTIST'),
            ('albumartist', 'ALBUMARTIST'),
            ('year', 'DATE'),
        ]

        for meta_key, vorbis_key in standard_tags:
            value = metadata.get(meta_key)
            if value:
                if overwrite or vorbis_key not in tags:
                    tags[vorbis_key] = [str(value)]

        # Custom tags (Vorbis comments are flexible)
        custom_tags = [
            ('series', 'SERIES'),
            ('series_num', 'SERIESNUMBER'),
            ('narrator', 'NARRATOR'),
            ('edition', 'EDITION'),
            ('variant', 'VARIANT'),
        ]

        for meta_key, vorbis_key in custom_tags:
            value = metadata.get(meta_key)
            if value:
                if overwrite or vorbis_key not in tags:
                    tags[vorbis_key] = [str(value)]

        audio.save()
        return True

    except Exception as e:
        logger.error(f"Failed to embed Vorbis tags in {file_path}: {e}")
        return False


def embed_tags_asf(file_path: Path, metadata: Dict[str, Any], overwrite: bool = True) -> bool:
    """Embed tags into WMA/ASF files."""
    try:
        from mutagen.asf import ASF

        audio = ASF(str(file_path))

        if audio.tags is None:
            audio.add_tags()

        tags = audio.tags

        # Standard ASF tags
        standard_tags = [
            ('title', 'Title'),
            ('album', 'WM/AlbumTitle'),
            ('artist', 'Author'),
            ('albumartist', 'WM/AlbumArtist'),
            ('year', 'WM/Year'),
        ]

        for meta_key, asf_key in standard_tags:
            value = metadata.get(meta_key)
            if value:
                if overwrite or asf_key not in tags:
                    tags[asf_key] = [str(value)]

        # Custom tags
        custom_tags = [
            ('series', 'WM/Series'),
            ('series_num', 'WM/SeriesNumber'),
            ('narrator', 'WM/Narrator'),
            ('edition', 'WM/Edition'),
            ('variant', 'WM/Variant'),
        ]

        for meta_key, asf_key in custom_tags:
            value = metadata.get(meta_key)
            if value:
                if overwrite or asf_key not in tags:
                    tags[asf_key] = [str(value)]

        audio.save()
        return True

    except Exception as e:
        logger.error(f"Failed to embed ASF tags in {file_path}: {e}")
        return False


def embed_tags(file_path: Path, metadata: Dict[str, Any], overwrite: bool = True) -> bool:
    """
    Embed metadata tags into an audio file.
    Dispatches to format-specific handler based on file extension.
    
    Args:
        file_path: Path to the audio file
        metadata: Dict with keys: title, album, artist, albumartist, year, 
                  series, series_num, narrator, edition, variant
        overwrite: If True, overwrite existing managed fields. If False, only fill missing.
    
    Returns:
        True if successful, False otherwise.
    """
    file_path = Path(file_path)
    ext = file_path.suffix.lower()

    if ext == '.mp3':
        return embed_tags_mp3(file_path, metadata, overwrite)
    elif ext in ('.m4b', '.m4a', '.aac'):
        return embed_tags_mp4(file_path, metadata, overwrite)
    elif ext in ('.flac', '.ogg', '.opus'):
        return embed_tags_vorbis(file_path, metadata, overwrite)
    elif ext == '.wma':
        return embed_tags_asf(file_path, metadata, overwrite)
    else:
        logger.debug(f"Unsupported format for tagging: {ext}")
        return False


def build_metadata_for_embedding(
    author: str,
    title: str,
    series: Optional[str] = None,
    series_num: Optional[str] = None,
    narrator: Optional[str] = None,
    year: Optional[str] = None,
    edition: Optional[str] = None,
    variant: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build a metadata dict suitable for embed_tags() from book info.
    Maps audiobook concepts to standard audio tag fields.
    """
    metadata = {
        'title': title,          # Track title (for single-file books) / Album title
        'album': title,          # Album = book title
        'artist': author,        # Artist = author (for compatibility)
        'albumartist': author,   # Album artist = author (more accurate for audiobooks)
    }

    if year:
        metadata['year'] = str(year)
    if series:
        metadata['series'] = series
    if series_num:
        metadata['series_num'] = str(series_num)
    if narrator:
        metadata['narrator'] = narrator
    if edition:
        metadata['edition'] = edition
    if variant:
        metadata['variant'] = variant

    return metadata


def embed_tags_for_path(
    target_path: Path,
    metadata: Dict[str, Any],
    create_backup: bool = True,
    overwrite: bool = True
) -> Dict[str, Any]:
    """
    High-level function to embed tags into all audio files at a path.
    Creates sidecar backup before modifying if requested.
    
    Args:
        target_path: File or folder path
        metadata: Dict with book metadata (author, title, series, etc.)
        create_backup: Whether to create/update sidecar backup
        overwrite: Whether to overwrite existing managed fields
    
    Returns:
        Dict with 'success': bool, 'files_processed': int, 'files_failed': int, 
        'error': str (if any critical error)
    """
    target = Path(target_path)
    result = {
        'success': True,
        'files_processed': 0,
        'files_failed': 0,
        'errors': []
    }

    try:
        # Collect audio files
        audio_files = collect_audio_files(target)
        if not audio_files:
            logger.debug(f"No audio files found at {target}")
            return result

        # Determine backup folder (parent of file, or the folder itself)
        backup_folder = target if target.is_dir() else target.parent

        # Create backups before modifying
        if create_backup:
            snapshots = []
            for f in audio_files:
                snap = snapshot_tags(f)
                if snap:
                    snapshots.append(snap)
            
            if snapshots:
                write_sidecar_backup(backup_folder, snapshots)

        # Embed tags
        for audio_file in audio_files:
            try:
                success = embed_tags(audio_file, metadata, overwrite)
                if success:
                    result['files_processed'] += 1
                else:
                    result['files_failed'] += 1
                    result['errors'].append(f"Failed to tag: {audio_file.name}")
            except Exception as e:
                result['files_failed'] += 1
                result['errors'].append(f"{audio_file.name}: {str(e)}")

        # Overall success if any files were processed
        result['success'] = result['files_processed'] > 0 or (len(audio_files) == 0)

    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
        logger.error(f"Error embedding tags at {target_path}: {e}")

    return result

