#!/usr/bin/env python3
"""
Library Metadata Manager - Web UI
Automatically fixes book metadata using AI.

Features:
- Web dashboard with stats
- Queue of books needing fixes
- History of all fixes made
- Settings management
- Multi-provider AI (Gemini, OpenRouter, Ollama)
"""

APP_VERSION = "0.9.0-beta.51"
GITHUB_REPO = "deucebucket/library-manager"  # Your GitHub repo

# Versioning Guide:
# 0.9.0-beta.1  = Initial beta (basic features)
# 0.9.0-beta.2  = Garbage filtering, series grouping, dismiss errors
# 0.9.0-beta.3  = UI cleanup - merged Advanced/Tools tabs
# 0.9.0-beta.4  = Improved series detection, DB locking fix, system folder filtering
# 0.9.0-beta.11 = Series folder lib_path fix
# 0.9.0-beta.12 = CRITICAL SAFETY: Path sanitization, library boundary checks, depth validation
# 0.9.0-rc.1    = Release candidate (feature complete, final testing)
# 1.0.0         = First stable release (everything works!)

import os
import sys
import json
import time
import sqlite3
import threading
import logging
import requests
import re
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file
from audio_tagging import embed_tags_for_path, build_metadata_for_embedding


# ============== BOOK PROFILE SYSTEM ==============
# Confidence-scored metadata profiles for comprehensive book identification

# Source weights for confidence calculation (higher = more trusted)
SOURCE_WEIGHTS = {
    'user': 100,        # Manual override - always wins
    'audio': 85,        # Heard directly from audiobook intro
    'id3': 80,          # Embedded by producer/publisher
    'json': 75,         # Explicit metadata file
    'nfo': 70,          # Release info files
    'bookdb': 65,       # Verified database match
    'ai': 60,           # AI verification
    'audnexus': 55,     # Audible data
    'googlebooks': 50,  # Google Books API
    'openlibrary': 45,  # OpenLibrary API
    'hardcover': 45,    # Hardcover API
    'path': 40          # Folder structure inference
}

# Field weights for overall confidence (must sum to 100)
FIELD_WEIGHTS = {
    'author': 30,
    'title': 30,
    'narrator': 15,
    'series': 10,
    'series_num': 5,
    'language': 5,
    'year': 3,
    'edition': 1,
    'variant': 1
}


@dataclass
class FieldValue:
    """A single metadata field with confidence and source tracking."""
    value: Any = None
    confidence: int = 0
    sources: List[str] = field(default_factory=list)
    raw_values: Dict[str, Any] = field(default_factory=dict)  # source -> raw value

    def add_source(self, source: str, value: Any, weight: int = None):
        """Add evidence from a source."""
        if value is None:
            return
        if weight is None:
            weight = SOURCE_WEIGHTS.get(source, 30)
        self.raw_values[source] = value
        if source not in self.sources:
            self.sources.append(source)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            'value': self.value,
            'confidence': self.confidence,
            'sources': self.sources
        }


@dataclass
class BookProfile:
    """Complete metadata profile for a book with per-field confidence."""
    # Core identification
    author: FieldValue = field(default_factory=FieldValue)
    title: FieldValue = field(default_factory=FieldValue)

    # Extended metadata
    narrator: FieldValue = field(default_factory=FieldValue)
    series: FieldValue = field(default_factory=FieldValue)
    series_num: FieldValue = field(default_factory=FieldValue)
    language: FieldValue = field(default_factory=FieldValue)
    year: FieldValue = field(default_factory=FieldValue)
    edition: FieldValue = field(default_factory=FieldValue)
    variant: FieldValue = field(default_factory=FieldValue)

    # Profile metadata
    overall_confidence: int = 0
    verification_layers_used: List[str] = field(default_factory=list)
    needs_attention: bool = False
    issues: List[str] = field(default_factory=list)
    last_updated: Optional[str] = None

    def calculate_field_confidence(self, fv: FieldValue) -> tuple:
        """Calculate confidence for a field based on source agreement."""
        if not fv.raw_values:
            return None, 0

        # Normalize values for comparison
        def normalize(val):
            if val is None:
                return None
            return str(val).lower().strip()

        # Group by normalized value
        value_groups = {}
        for source, value in fv.raw_values.items():
            if value is None:
                continue
            normalized = normalize(value)
            if normalized not in value_groups:
                value_groups[normalized] = []
            weight = SOURCE_WEIGHTS.get(source, 30)
            value_groups[normalized].append((source, value, weight))

        if not value_groups:
            return None, 0

        # Find consensus value (highest total weight)
        best_value = None
        best_weight = 0
        best_normalized = None
        for normalized, sources in value_groups.items():
            total_weight = sum(w for _, _, w in sources)
            if total_weight > best_weight:
                best_weight = total_weight
                best_normalized = normalized
                # Use original value from highest-weighted source
                best_source = max(sources, key=lambda x: x[2])
                best_value = best_source[1]

        # Calculate confidence
        agreeing_sources = len(value_groups.get(best_normalized, []))
        conflicting_values = len(value_groups) - 1

        base_confidence = min(best_weight, 100)

        # Agreement bonus
        if agreeing_sources >= 4:
            base_confidence = min(base_confidence + 25, 100)
        elif agreeing_sources >= 3:
            base_confidence = min(base_confidence + 20, 100)
        elif agreeing_sources >= 2:
            base_confidence = min(base_confidence + 10, 100)

        # Conflict penalty
        base_confidence = max(base_confidence - (conflicting_values * 15), 0)

        return best_value, round(base_confidence)

    def finalize(self):
        """Calculate final values and confidence for all fields."""
        for field_name in FIELD_WEIGHTS.keys():
            fv = getattr(self, field_name)
            if fv.raw_values:
                value, confidence = self.calculate_field_confidence(fv)
                fv.value = value
                fv.confidence = confidence

        self.calculate_overall_confidence()
        self.last_updated = datetime.now().isoformat()

    def calculate_overall_confidence(self) -> int:
        """Calculate weighted overall confidence from field confidences."""
        total_weight = 0
        weighted_sum = 0

        for field_name, weight in FIELD_WEIGHTS.items():
            fv = getattr(self, field_name)
            if fv.value is not None:
                weighted_sum += fv.confidence * weight
                total_weight += weight

        if total_weight == 0:
            self.overall_confidence = 0
        else:
            self.overall_confidence = round(weighted_sum / total_weight)

        return self.overall_confidence

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        result = {}
        for field_name in FIELD_WEIGHTS.keys():
            fv = getattr(self, field_name)
            result[field_name] = fv.to_dict()
        result['overall_confidence'] = self.overall_confidence
        result['verification_layers_used'] = self.verification_layers_used
        result['needs_attention'] = self.needs_attention
        result['issues'] = self.issues
        result['last_updated'] = self.last_updated
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BookProfile':
        """Reconstruct from JSON dictionary."""
        profile = cls()
        for field_name in FIELD_WEIGHTS.keys():
            if field_name in data:
                fd = data[field_name]
                fv = FieldValue(
                    value=fd.get('value'),
                    confidence=fd.get('confidence', 0),
                    sources=fd.get('sources', [])
                )
                setattr(profile, field_name, fv)
        profile.overall_confidence = data.get('overall_confidence', 0)
        profile.verification_layers_used = data.get('verification_layers_used', [])
        profile.needs_attention = data.get('needs_attention', False)
        profile.issues = data.get('issues', [])
        profile.last_updated = data.get('last_updated')
        return profile


def detect_multibook_vs_chapters(audio_files: List[Path], config: dict = None) -> dict:
    """
    Determine if numbered audio files are multiple books or chapter files.

    Fix for Issue #29: Files like "00 - Chapter.mp3", "01 - Prologue.mp3" were
    incorrectly flagged as multibook collections because the regex matched
    leading numbers. Chapter files are NOT multiple books.

    Returns:
        {
            'is_multibook': bool,
            'confidence': 'high'|'medium'|'low',
            'reason': str,
            'book_numbers': set (if multibook)
        }
    """
    if not audio_files or len(audio_files) < 2:
        return {'is_multibook': False, 'confidence': 'high', 'reason': 'Less than 2 files'}

    # Chapter indicators - files with these are NEVER part of a multibook collection
    chapter_indicators = [
        r'chapter\s*\d*',
        r'ch\.?\s*\d+',
        r'part\s*\d+',
        r'prologue',
        r'epilogue',
        r'intro(duction)?',
        r'outro',
        r'disc\s*\d+',
        r'cd\s*\d+',
        r'track\s*\d+',
        r'section\s*\d+',
    ]

    # Explicit book patterns - these DO indicate multiple books
    book_patterns = [
        (r'book\s*(\d+)', 'book'),           # "Book 1", "Book 2"
        (r'volume\s*(\d+)', 'volume'),       # "Volume 1"
        (r'vol\.?\s*(\d+)', 'vol'),          # "Vol 1", "Vol. 1"
        (r'#(\d+)\s*[-–—:]', 'hashtag'),     # "#1 - Title" (with separator)
    ]

    files_with_chapter_indicator = 0
    book_numbers_found = set()
    leading_numbers = []

    for f in audio_files:
        stem = f.stem.lower()

        # Check for chapter indicators
        has_chapter = any(re.search(p, stem, re.IGNORECASE) for p in chapter_indicators)
        if has_chapter:
            files_with_chapter_indicator += 1
            continue  # Don't check book patterns for chapter files

        # Check for explicit book patterns
        for pattern, pattern_type in book_patterns:
            m = re.search(pattern, stem, re.IGNORECASE)
            if m:
                book_numbers_found.add(m.group(1))
                break

        # Track leading numbers for sequential analysis
        leading_match = re.match(r'^(\d+)', stem)
        if leading_match:
            leading_numbers.append(int(leading_match.group(1)))

    # Decision logic

    # If majority have chapter indicators, definitely chapters
    if files_with_chapter_indicator > len(audio_files) * 0.3:
        return {
            'is_multibook': False,
            'confidence': 'high',
            'reason': f'{files_with_chapter_indicator}/{len(audio_files)} files have chapter indicators'
        }

    # If explicit book patterns found multiple books
    if len(book_numbers_found) >= 2:
        return {
            'is_multibook': True,
            'confidence': 'high',
            'reason': f'Found explicit book numbers: {sorted(book_numbers_found)}',
            'book_numbers': book_numbers_found
        }

    # Check if leading numbers are sequential (suggests chapters, not books)
    if leading_numbers:
        leading_numbers.sort()
        # Check if starts from 0 or 1 and is mostly sequential
        if leading_numbers[0] in [0, 1]:
            # Check for reasonable sequential pattern (allow small gaps)
            is_sequential = True
            for i in range(1, len(leading_numbers)):
                gap = leading_numbers[i] - leading_numbers[i-1]
                if gap > 2:  # Allow gaps up to 2 (missing chapter)
                    is_sequential = False
                    break

            if is_sequential:
                return {
                    'is_multibook': False,
                    'confidence': 'medium',
                    'reason': f'Sequential numbering from {leading_numbers[0]} suggests chapters'
                }

    # Default: if no explicit book patterns found, assume chapters
    return {
        'is_multibook': False,
        'confidence': 'medium',
        'reason': 'No explicit book patterns found, defaulting to chapters'
    }


def save_book_profile(book_id: int, profile: BookProfile):
    """Save a book profile to the database."""
    conn = get_db()
    c = conn.cursor()
    try:
        profile_json = json.dumps(profile.to_dict())
        c.execute('''UPDATE books
                     SET profile = ?, confidence = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?''',
                  (profile_json, profile.overall_confidence, book_id))
        conn.commit()
    finally:
        conn.close()


def load_book_profile(book_id: int) -> Optional[BookProfile]:
    """Load a book profile from the database."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT profile FROM books WHERE id = ?', (book_id,))
        row = c.fetchone()
        if row and row['profile']:
            try:
                data = json.loads(row['profile'])
                return BookProfile.from_dict(data)
            except:
                return None
        return None
    finally:
        conn.close()


def build_profile_from_sources(
    path_info: dict = None,
    folder_meta: dict = None,
    api_candidates: list = None,
    ai_result: dict = None,
    audio_result: dict = None
) -> BookProfile:
    """
    Build a BookProfile by combining data from multiple sources.
    Each source adds evidence to the profile's fields.
    """
    profile = BookProfile()

    # Layer 1: Path analysis
    if path_info:
        profile.verification_layers_used.append('local')
        if path_info.get('detected_author'):
            profile.author.add_source('path', path_info['detected_author'])
        if path_info.get('detected_title'):
            profile.title.add_source('path', path_info['detected_title'])
        if path_info.get('detected_series'):
            profile.series.add_source('path', path_info['detected_series'])
        if path_info.get('issues'):
            profile.issues.extend(path_info['issues'])

    # Layer 1: Folder metadata (ID3, NFO, JSON)
    if folder_meta:
        if folder_meta.get('audio_author'):
            profile.author.add_source('id3', folder_meta['audio_author'])
        if folder_meta.get('audio_title'):
            profile.title.add_source('id3', folder_meta['audio_title'])
        if folder_meta.get('nfo_author'):
            profile.author.add_source('nfo', folder_meta['nfo_author'])
        if folder_meta.get('nfo_title'):
            profile.title.add_source('nfo', folder_meta['nfo_title'])
        if folder_meta.get('nfo_narrator'):
            profile.narrator.add_source('nfo', folder_meta['nfo_narrator'])
        if folder_meta.get('meta_author'):
            profile.author.add_source('json', folder_meta['meta_author'])
        if folder_meta.get('meta_title'):
            profile.title.add_source('json', folder_meta['meta_title'])
        if folder_meta.get('meta_narrator'):
            profile.narrator.add_source('json', folder_meta['meta_narrator'])

    # Layer 2: API candidates
    if api_candidates:
        if 'api' not in profile.verification_layers_used:
            profile.verification_layers_used.append('api')
        for candidate in api_candidates:
            source = candidate.get('source', 'api')
            if candidate.get('author'):
                profile.author.add_source(source, candidate['author'])
            if candidate.get('title'):
                profile.title.add_source(source, candidate['title'])
            if candidate.get('series'):
                profile.series.add_source(source, candidate['series'])
            if candidate.get('year'):
                profile.year.add_source(source, candidate['year'])

    # Layer 3: AI result
    if ai_result:
        if 'ai' not in profile.verification_layers_used:
            profile.verification_layers_used.append('ai')
        if ai_result.get('author'):
            profile.author.add_source('ai', ai_result['author'])
        if ai_result.get('title'):
            profile.title.add_source('ai', ai_result['title'])
        if ai_result.get('narrator'):
            profile.narrator.add_source('ai', ai_result['narrator'])
        if ai_result.get('series'):
            profile.series.add_source('ai', ai_result['series'])
        if ai_result.get('series_num'):
            profile.series_num.add_source('ai', ai_result['series_num'])
        if ai_result.get('year'):
            profile.year.add_source('ai', ai_result['year'])
        if ai_result.get('edition'):
            profile.edition.add_source('ai', ai_result['edition'])
        if ai_result.get('variant'):
            profile.variant.add_source('ai', ai_result['variant'])

    # Layer 4: Audio analysis
    if audio_result:
        if 'audio' not in profile.verification_layers_used:
            profile.verification_layers_used.append('audio')
        if audio_result.get('author'):
            profile.author.add_source('audio', audio_result['author'])
        if audio_result.get('title'):
            profile.title.add_source('audio', audio_result['title'])
        if audio_result.get('narrator'):
            profile.narrator.add_source('audio', audio_result['narrator'])
        if audio_result.get('series'):
            profile.series.add_source('audio', audio_result['series'])
        if audio_result.get('language'):
            profile.language.add_source('audio', audio_result['language'])

    # Finalize - calculate consensus values and confidence
    profile.finalize()

    # Flag low confidence for attention
    if profile.overall_confidence < 50:
        profile.needs_attention = True
        if 'low_confidence' not in profile.issues:
            profile.issues.append('low_confidence')

    return profile


# ============== SEARCH QUEUE / PROGRESS TRACKING ==============
# Tracks progress of chaos scans and batch operations for UI feedback

class SearchProgress:
    """Thread-safe progress tracker for chaos handler and batch operations."""

    def __init__(self):
        self._lock = threading.Lock()
        self._state = {
            'active': False,
            'operation': None,
            'total': 0,
            'processed': 0,
            'current_item': None,
            'status_message': None,  # Real-time status for user feedback
            'results': [],
            'started_at': None,
            'queue': []  # Items waiting to be processed
        }

    def set_status(self, message):
        """Set current status message for user feedback (e.g., 'BookDB timeout, trying AI...')"""
        with self._lock:
            self._state['status_message'] = message

    def start(self, operation, total, queue_items=None):
        """Start a new operation."""
        with self._lock:
            self._state = {
                'active': True,
                'operation': operation,
                'total': total,
                'processed': 0,
                'current_item': None,
                'status_message': 'Starting...',
                'results': [],
                'started_at': datetime.now().isoformat(),
                'queue': queue_items or []
            }

    def update(self, current_item, result=None):
        """Update progress with current item being processed."""
        with self._lock:
            self._state['processed'] += 1
            self._state['current_item'] = current_item
            if result:
                self._state['results'].append(result)
            # Remove from queue
            if current_item in self._state['queue']:
                self._state['queue'].remove(current_item)

    def finish(self):
        """Mark operation as complete."""
        with self._lock:
            self._state['active'] = False
            self._state['current_item'] = None
            self._state['queue'] = []

    def get_state(self):
        """Get current progress state."""
        with self._lock:
            state = self._state.copy()
            if state['total'] > 0:
                state['percent'] = round((state['processed'] / state['total']) * 100, 1)
            else:
                state['percent'] = 0
            state['queue_position'] = len(state['queue'])
            return state

# Global progress tracker
search_progress = SearchProgress()


# ============== LOCAL BOOKDB CONNECTION ==============
# Direct SQLite connection to local metadata database for fast lookups

BOOKDB_LOCAL_PATH = "/mnt/rag_data/bookdb/metadata.db"

def get_bookdb_connection():
    """Get a connection to the local BookDB SQLite database."""
    if os.path.exists(BOOKDB_LOCAL_PATH):
        try:
            return sqlite3.connect(BOOKDB_LOCAL_PATH, timeout=5)
        except Exception as e:
            logging.debug(f"Could not connect to local BookDB: {e}")
    return None


# ============== SMART MATCHING UTILITIES ==============

def calculate_title_similarity(title1, title2):
    """
    Calculate word overlap similarity between two titles.
    Returns a score from 0.0 to 1.0
    """
    if not title1 or not title2:
        return 0.0

    # Normalize: lowercase, remove punctuation, split into words
    def normalize(t):
        t = t.lower()
        t = re.sub(r'[^\w\s]', ' ', t)
        words = set(t.split())
        # Remove common stop words that don't help matching
        stop_words = {'the', 'a', 'an', 'of', 'and', 'or', 'in', 'to', 'for', 'by', 'part', 'book', 'volume'}
        return words - stop_words

    words1 = normalize(title1)
    words2 = normalize(title2)

    if not words1 or not words2:
        return 0.0

    # Calculate Jaccard similarity (intersection over union)
    intersection = words1 & words2
    union = words1 | words2

    return len(intersection) / len(union) if union else 0.0


def extract_series_from_title(title):
    """
    Extract series name and number from title patterns like:
    - "The Firefly Series, Book 8: Coup de Grâce" -> (Firefly, 8, Coup de Grâce)
    - "The Firefly Series, Book 8꞉ Firefly꞉ Coup de Grâce" -> (Firefly, 8, Firefly: Coup de Grâce)
    - "Mistborn Book 1: The Final Empire" -> (Mistborn, 1, The Final Empire)
    - "The Expanse #3 - Abaddon's Gate" -> (The Expanse, 3, Abaddon's Gate)
    """
    # Normalize colon-like characters (Windows uses ꞉ instead of : in filenames)
    normalized = title.replace('꞉', ':').replace('：', ':')  # U+A789 and full-width colon

    # Pattern: "Series Name, Book N: Title" or "Series Name Book N: Title"
    # Also handles "The X Series, Book N: Title"
    match = re.search(r'^(?:The\s+)?(.+?)\s*(?:Series)?,?\s*Book\s+(\d+)\s*[:\s-]+(.+)$', normalized, re.IGNORECASE)
    if match:
        series = match.group(1).strip()
        # Clean up series name (remove trailing "Series" if it got in)
        series = re.sub(r'\s*Series\s*$', '', series, flags=re.IGNORECASE)
        return series, int(match.group(2)), match.group(3).strip()

    # Pattern: "Series #N - Title" or "Series #N: Title"
    match = re.search(r'^(.+?)\s*#(\d+)\s*[:\s-]+(.+)$', normalized)
    if match:
        return match.group(1).strip(), int(match.group(2)), match.group(3).strip()

    # Pattern: "Series Book N - Title"
    match = re.search(r'^(.+?)\s+Book\s+(\d+)\s*[:\s-]+(.+)$', normalized, re.IGNORECASE)
    if match:
        return match.group(1).strip(), int(match.group(2)), match.group(3).strip()

    # Pattern: "Series Book N" at END (no subtitle) - e.g., "Dark One Book 1"
    # Series name = title before "Book N", actual title = same as series
    match = re.search(r'^(.+?)\s+Book\s+(\d+)\s*$', normalized, re.IGNORECASE)
    if match:
        series = match.group(1).strip()
        return series, int(match.group(2)), series  # Title = series name

    # Pattern: "Series #N" at END (no subtitle) - e.g., "Mistborn #1"
    match = re.search(r'^(.+?)\s*#(\d+)\s*$', normalized)
    if match:
        series = match.group(1).strip()
        return series, int(match.group(2)), series

    # Pattern: "Title (Book N)" - book number in parentheses at end
    # e.g., "Ivypool's Heart (Book 17)" -> extract number, title stays same
    match = re.search(r'^(.+?)\s*\(Book\s+(\d+)\)\s*$', normalized, re.IGNORECASE)
    if match:
        title_clean = match.group(1).strip()
        return None, int(match.group(2)), title_clean  # Series unknown, just got number

    return None, None, title


def is_unsearchable_query(title):
    """
    Check if a title is clearly not a book title and shouldn't be searched.
    Returns True for things like:
    - chapter1, chapter2, Chapter 19
    - 01.mp3, track_05
    - audiobook, full audiobook
    - disc1, cd2, part3
    """
    if not title:
        return True

    title_lower = title.lower().strip()

    # Just numbers (01, 001, 1)
    if re.match(r'^\d+$', title_lower):
        return True

    # Chapter + number patterns (chapter1, chapter 5, ch01)
    if re.match(r'^(?:chapter|ch|chap)\s*\d+$', title_lower):
        return True

    # Track/disc/part patterns (track01, disc2, part 3, cd1)
    if re.match(r'^(?:track|disc|cd|part|pt)\s*\d+$', title_lower):
        return True

    # Just "audiobook" or "full audiobook"
    if re.match(r'^(?:full\s+)?audiobook$', title_lower):
        return True

    # Very short titles (1-2 chars) are usually garbage
    if len(title_lower) <= 2:
        return True

    return False


def is_garbage_match(original_title, suggested_title, threshold=0.3):
    """
    Check if an API suggestion is garbage (very low title similarity).
    Returns True if the match should be rejected.

    Examples that should be rejected:
    - "Chapter 19" -> "College Accounting, Chapters 1-9" (only matches "chapter")
    - "Death Genesis" -> "The Darkborn AfterLife Genesis" (only matches "genesis")
    - "Mr. Murder" -> "Frankenstein" (no overlap)

    Threshold of 0.3 means at least 30% word overlap required.
    """
    similarity = calculate_title_similarity(original_title, suggested_title)

    # If original is very short (1-2 words), be more lenient
    orig_words = len([w for w in original_title.lower().split() if len(w) > 2])
    if orig_words <= 2 and similarity >= 0.2:
        return False

    if similarity < threshold:
        logger.info(f"Garbage match rejected: '{original_title}' vs '{suggested_title}' (similarity: {similarity:.2f})")
        return True

    return False


def extract_folder_metadata(folder_path):
    """
    Extract metadata clues from files in the book folder.
    Looks for: .nfo files, cover images with text, metadata files
    Returns dict with any found metadata hints.
    """
    hints = {}
    folder = Path(folder_path)

    if not folder.exists():
        return hints

    # Look for .nfo files (common in audiobook releases)
    nfo_files = list(folder.glob('*.nfo')) + list(folder.glob('*.NFO'))
    for nfo in nfo_files:
        try:
            content = nfo.read_text(errors='ignore')
            # Look for author/title patterns in NFO
            author_match = re.search(r'(?:author|by|written by)[:\s]+([^\n\r]+)', content, re.IGNORECASE)
            title_match = re.search(r'(?:title|book)[:\s]+([^\n\r]+)', content, re.IGNORECASE)
            if author_match:
                hints['nfo_author'] = author_match.group(1).strip()
            if title_match:
                hints['nfo_title'] = title_match.group(1).strip()
        except Exception:
            pass

    # Look for metadata.json or info.json
    for meta_file in ['metadata.json', 'info.json', 'audiobook.json']:
        meta_path = folder / meta_file
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                if 'author' in meta:
                    hints['meta_author'] = meta['author']
                if 'title' in meta:
                    hints['meta_title'] = meta['title']
                if 'narrator' in meta:
                    hints['meta_narrator'] = meta['narrator']
            except Exception:
                pass

    # Look for desc.txt or description.txt
    for desc_file in ['desc.txt', 'description.txt', 'readme.txt']:
        desc_path = folder / desc_file
        if desc_path.exists():
            try:
                content = desc_path.read_text(errors='ignore')[:2000]  # First 2KB
                hints['description'] = content
            except Exception:
                pass

    # Check audio file metadata using mutagen (if available)
    audio_files = list(folder.glob('*.m4b')) + list(folder.glob('*.mp3')) + list(folder.glob('*.m4a'))
    if audio_files:
        try:
            from mutagen import File
            audio = File(audio_files[0], easy=True)
            if audio:
                if 'albumartist' in audio:
                    hints['audio_author'] = audio['albumartist'][0]
                elif 'artist' in audio:
                    hints['audio_author'] = audio['artist'][0]
                if 'album' in audio:
                    hints['audio_title'] = audio['album'][0]
        except Exception:
            pass

    return hints


def extract_narrator_from_folder(folder_path):
    """
    Try to extract narrator information from audio files in a folder.
    Checks multiple sources: ID3 tags, NFO files, metadata files.
    Returns narrator name string or None if not found.
    """
    folder = Path(folder_path)
    if not folder.exists():
        return None

    # First, try audio file tags
    audio_extensions = ['.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus']
    audio_files = []
    for ext in audio_extensions:
        audio_files.extend(folder.glob(f'*{ext}'))
        audio_files.extend(folder.glob(f'*{ext.upper()}'))

    if audio_files:
        try:
            from mutagen import File
            from mutagen.mp3 import MP3
            from mutagen.mp4 import MP4

            audio = File(audio_files[0])
            if audio:
                # MP4/M4B - check for narrator in various fields
                if hasattr(audio, 'tags') and audio.tags:
                    # M4B/M4A often have narrator in '----:com.apple.iTunes:NARRATOR' or similar
                    for key in audio.tags.keys():
                        key_lower = str(key).lower()
                        if 'narrator' in key_lower or 'read by' in key_lower:
                            val = audio.tags[key]
                            if hasattr(val, 'text'):
                                return str(val.text[0]) if val.text else None
                            elif isinstance(val, list) and val:
                                return str(val[0])
                            else:
                                return str(val)

                # Check 'composer' field - sometimes used for narrator
                if 'composer' in audio:
                    return audio['composer'][0]
                if '\xa9wrt' in audio:  # MP4 composer
                    return audio['\xa9wrt'][0]

        except Exception as e:
            logging.debug(f"Could not extract narrator from audio: {e}")

    # Try NFO files - often have narrator info
    nfo_files = list(folder.glob('*.nfo')) + list(folder.glob('*.NFO'))
    for nfo in nfo_files:
        try:
            content = nfo.read_text(errors='ignore')
            # Look for narrator patterns
            narrator_match = re.search(
                r'(?:narrator|narrated by|read by|performed by|reader)[:\s]+([^\n\r]+)',
                content, re.IGNORECASE
            )
            if narrator_match:
                narrator = narrator_match.group(1).strip()
                # Clean up common suffixes
                narrator = re.sub(r'\s*\(.*\)$', '', narrator)
                if narrator and len(narrator) > 2:
                    return narrator
        except Exception:
            pass

    # Try metadata files
    for meta_file in ['metadata.json', 'info.json', 'audiobook.json', 'book.json']:
        meta_path = folder / meta_file
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                for key in ['narrator', 'narrators', 'read_by', 'reader', 'performed_by']:
                    if key in meta:
                        val = meta[key]
                        if isinstance(val, list):
                            return val[0] if val else None
                        return val
            except Exception:
                pass

    return None


# Configure logging - use script directory for log file
APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(APP_DIR, 'app.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Silence Flask's HTTP request logging (only show errors)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)
app.secret_key = 'library-manager-secret-key-2024'

# ============== CONFIGURATION ==============

BASE_DIR = Path(__file__).parent

# Support DATA_DIR env var for Docker persistence
# Auto-detect common Docker mount points if not explicitly set
# UnRaid uses /config, our default is /data
def _detect_data_dir():
    """Auto-detect the data directory for Docker persistence.

    Priority:
    1. Explicit DATA_DIR env var (user override)
    2. Directory with existing config files (preserves user settings)
    3. Mounted volume (detects actual Docker mounts vs container dirs)
    4. /data for fresh installs (our documented default)
    5. /config if /data doesn't exist (UnRaid fallback)
    6. App directory (local development)
    """
    # If explicitly set via env var, use that
    if 'DATA_DIR' in os.environ:
        return Path(os.environ['DATA_DIR'])

    # Check for existing config files - NEVER lose user settings
    # Check both locations, prefer whichever has config
    for mount_point in ['/data', '/config']:
        mount_path = Path(mount_point)
        if mount_path.exists() and mount_path.is_dir():
            if (mount_path / 'config.json').exists() or (mount_path / 'library.db').exists():
                return mount_path

    # Fresh install: detect which is actually mounted (persistent storage)
    # os.path.ismount() returns True for Docker volume mounts
    if os.path.ismount('/data'):
        return Path('/data')
    if os.path.ismount('/config'):
        return Path('/config')

    # Fallback: prefer /data (our documented default), then /config
    if Path('/data').exists():
        return Path('/data')
    if Path('/config').exists():
        return Path('/config')

    # Fall back to app directory (local development)
    return BASE_DIR

DATA_DIR = _detect_data_dir()
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Log where we're storing data (helps debug mount issues)
import logging
_startup_logger = logging.getLogger(__name__)
_startup_logger.info(f"Data directory: {DATA_DIR} (config persistence location)")

DB_PATH = DATA_DIR / 'library.db'
CONFIG_PATH = DATA_DIR / 'config.json'
SECRETS_PATH = DATA_DIR / 'secrets.json'

DEFAULT_CONFIG = {
    "library_paths": [],  # Empty by default - user configures via Settings
    "ai_provider": "openrouter",  # "openrouter", "gemini", or "ollama"
    "openrouter_model": "google/gemma-3n-e4b-it:free",
    "gemini_model": "gemini-2.0-flash",
    "ollama_url": "http://localhost:11434",  # Ollama server URL
    "ollama_model": "llama3.2:3b",  # Default model - good for 8-12GB VRAM
    "scan_interval_hours": 6,
    "batch_size": 3,
    "max_requests_per_hour": 30,
    "auto_fix": False,
    "protect_author_changes": True,  # Require manual approval when author changes completely
    "enabled": True,
    "series_grouping": False,  # Group series: Author/Series/1 - Title (Audiobookshelf compatible)
    "ebook_management": False,  # Enable ebook organization (Beta)
    "ebook_library_mode": "merge",  # "merge" = same folder as audiobooks, "separate" = own library
    "update_channel": "beta",  # "stable", "beta", or "nightly"
    "naming_format": "author/title",  # "author/title", "author - title", "custom"
    "custom_naming_template": "{author}/{title}",  # Custom template with {author}, {title}, {series}, etc.
    # Metadata embedding settings
    "metadata_embedding_enabled": False,  # Embed tags into audio files when fixes are applied
    "metadata_embedding_overwrite_managed": True,  # Overwrite managed fields (title/author/series/etc)
    "metadata_embedding_backup_sidecar": True,  # Create .library-manager.tags.json backup before modifying
    # Language preference settings (Issue #17)
    "preferred_language": "en",  # ISO 639-1 code for metadata lookups
    "preserve_original_titles": True,  # Don't replace foreign language titles with English translations
    "detect_language_from_audio": False,  # Use Gemini audio analysis to detect spoken language
    # Trust the Process mode - fully automatic verification chain
    "trust_the_process": False,  # Auto-verify drastic changes, use audio analysis as tie-breaker, only flag truly unidentifiable
    # Book Profile System settings - progressive verification with confidence scoring
    "enable_api_lookups": True,           # Layer 2: API database lookups (BookDB, Audnexus, etc.)
    "enable_ai_verification": True,       # Layer 3: AI verification (uses configured provider)
    "enable_audio_analysis": False,       # Layer 4: Audio analysis (requires Gemini API key)
    "deep_scan_mode": False,              # Always use all enabled layers regardless of confidence
    "profile_confidence_threshold": 85,   # Minimum confidence to skip remaining layers (0-100)
    "multibook_ai_fallback": True,         # Use AI for ambiguous chapter/multibook detection
    "skip_confirmations": False,           # Skip confirmation dialogs in Library view for faster workflow
    # Anonymous error reporting - helps improve the app
    "anonymous_error_reporting": False,    # Opt-in: send anonymous error reports to help fix bugs
    "error_reporting_include_titles": True # Include book title/author ONLY when they caused the error
}

DEFAULT_SECRETS = {
    "openrouter_api_key": "",
    "gemini_api_key": "",
    "abs_api_token": ""
}


def migrate_legacy_config():
    """Migrate config files from old locations to DATA_DIR.

    Issue #23: In beta.23 we fixed config paths to use DATA_DIR instead of BASE_DIR,
    but didn't migrate existing configs. Users updating from older versions would
    lose their config because the app looked in the new location.

    Also checks /config and /data for UnRaid vs standard Docker setups.
    """
    if DATA_DIR == BASE_DIR:
        return  # Not running with separate data dir, nothing to migrate

    import shutil
    migrate_files = ['config.json', 'secrets.json', 'library.db', 'user_groups.json']

    # Check multiple possible legacy locations
    # - BASE_DIR: old versions stored in app directory
    # - /data: our default Docker mount (user might have switched to /config)
    # - /config: UnRaid default (user might have switched to /data)
    legacy_locations = [BASE_DIR]
    if DATA_DIR != Path('/data') and Path('/data').exists():
        legacy_locations.append(Path('/data'))
    if DATA_DIR != Path('/config') and Path('/config').exists():
        legacy_locations.append(Path('/config'))

    for filename in migrate_files:
        new_path = DATA_DIR / filename

        # Skip if already exists in target location
        if new_path.exists():
            continue

        # Try to find file in legacy locations
        for legacy_dir in legacy_locations:
            old_path = legacy_dir / filename
            if old_path.exists():
                try:
                    shutil.copy2(old_path, new_path)
                    logger.info(f"Migrated {filename} from {old_path} to {new_path}")
                    break  # Found and migrated, stop looking
                except Exception as e:
                    logger.warning(f"Failed to migrate {filename} from {old_path}: {e}")


def init_config():
    """Create default config files if they don't exist."""
    if not CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'w') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        logger.info(f"Created default config at {CONFIG_PATH}")

    if not SECRETS_PATH.exists():
        with open(SECRETS_PATH, 'w') as f:
            json.dump(DEFAULT_SECRETS, f, indent=2)
        logger.info(f"Created default secrets at {SECRETS_PATH}")

# ============== DATABASE ==============

def init_db():
    """Initialize SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Books table - tracks all scanned books
    c.execute('''CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY,
        path TEXT UNIQUE,
        current_author TEXT,
        current_title TEXT,
        status TEXT DEFAULT 'pending',
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Add error_message column if it doesn't exist (migration)
    try:
        c.execute('ALTER TABLE books ADD COLUMN error_message TEXT')
    except:
        pass  # Column already exists

    # Queue table - books needing AI analysis
    c.execute('''CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY,
        book_id INTEGER,
        priority INTEGER DEFAULT 5,
        reason TEXT,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (book_id) REFERENCES books(id)
    )''')

    # History table - all fixes made
    c.execute('''CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY,
        book_id INTEGER,
        old_author TEXT,
        old_title TEXT,
        new_author TEXT,
        new_title TEXT,
        old_path TEXT,
        new_path TEXT,
        status TEXT DEFAULT 'pending_fix',
        error_message TEXT,
        fixed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (book_id) REFERENCES books(id)
    )''')

    # Add status and error_message columns if they don't exist (migration)
    try:
        c.execute("ALTER TABLE history ADD COLUMN status TEXT DEFAULT 'pending_fix'")
    except:
        pass
    try:
        c.execute('ALTER TABLE history ADD COLUMN error_message TEXT')
    except:
        pass

    # Add metadata columns for embedding (migration)
    metadata_columns = [
        'new_narrator TEXT',
        'new_series TEXT',
        'new_series_num TEXT',
        'new_year TEXT',
        'new_edition TEXT',
        'new_variant TEXT',
        'embed_status TEXT',
        'embed_error TEXT'
    ]
    for col_def in metadata_columns:
        try:
            c.execute(f'ALTER TABLE history ADD COLUMN {col_def}')
        except:
            pass  # Column already exists

    # Add profile columns for Book Profile system (migration)
    profile_columns = [
        ('books', 'profile TEXT'),          # Full JSON profile
        ('books', 'confidence INTEGER DEFAULT 0'),  # Overall confidence score
        ('books', 'verification_layer INTEGER DEFAULT 0')  # 0=pending, 1=API, 2=AI, 3=audio, 4=complete
    ]
    for table, col_def in profile_columns:
        try:
            c.execute(f'ALTER TABLE {table} ADD COLUMN {col_def}')
        except:
            pass  # Column already exists

    # Stats table - daily stats
    c.execute('''CREATE TABLE IF NOT EXISTS stats (
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE,
        scanned INTEGER DEFAULT 0,
        queued INTEGER DEFAULT 0,
        fixed INTEGER DEFAULT 0,
        verified INTEGER DEFAULT 0,
        api_calls INTEGER DEFAULT 0
    )''')

    conn.commit()
    conn.close()

def get_db():
    """Get database connection with timeout to avoid lock issues."""
    conn = sqlite3.connect(DB_PATH, timeout=30)  # Wait up to 30 seconds for lock
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')  # Better concurrent access
    return conn

# ============== CONFIG ==============

def load_config():
    """Load configuration and secrets from files."""
    config = DEFAULT_CONFIG.copy()

    # Load main config
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            logger.warning(f"Error loading config: {e}")

    # Load secrets (API keys)
    if SECRETS_PATH.exists():
        try:
            with open(SECRETS_PATH) as f:
                secrets = json.load(f)
                config.update(secrets)
        except Exception as e:
            logger.warning(f"Error loading secrets: {e}")

    return config

def save_config(config):
    """Save configuration to file (excludes secrets)."""
    # Separate secrets from config
    secrets_keys = ['openrouter_api_key', 'gemini_api_key', 'abs_api_token']
    config_only = {k: v for k, v in config.items() if k not in secrets_keys}

    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_only, f, indent=2)


def save_secrets(secrets):
    """Save API keys to secrets file."""
    with open(SECRETS_PATH, 'w') as f:
        json.dump(secrets, f, indent=2)

def load_secrets():
    """Load API keys from secrets file."""
    if SECRETS_PATH.exists():
        try:
            with open(SECRETS_PATH) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading secrets: {e}")
    return {}

# ============== ANONYMOUS ERROR REPORTING ==============

ERROR_REPORTS_PATH = DATA_DIR / "error_reports.json"

def sanitize_error_data(error_msg: str, traceback_str: str = None, book_info: dict = None) -> dict:
    """
    Sanitize error data to remove any personal/sensitive information.

    Removes:
    - File system paths (except relative library structure)
    - API keys and tokens
    - Usernames and home directories
    - IP addresses
    - Any config values that might contain secrets

    Keeps:
    - Error type and message (sanitized)
    - App version
    - Book title/author (only if enabled and relevant to error)
    """
    import re

    # Patterns to remove
    path_pattern = r'(/home/[^/\s]+|/Users/[^/\s]+|/data|/audiobooks|[A-Z]:\\[^\s]+)'
    ip_pattern = r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
    api_key_pattern = r'(sk-[a-zA-Z0-9]+|AIza[a-zA-Z0-9_-]+|[a-zA-Z0-9]{32,})'

    def sanitize_string(s: str) -> str:
        if not s:
            return s
        # Replace paths with [PATH]
        s = re.sub(path_pattern, '[PATH]', s)
        # Replace IPs with [IP]
        s = re.sub(ip_pattern, '[IP]', s)
        # Replace potential API keys with [KEY]
        s = re.sub(api_key_pattern, '[KEY]', s)
        return s

    sanitized = {
        "version": APP_VERSION,
        "timestamp": datetime.now().isoformat(),
        "error": sanitize_string(str(error_msg)[:500]),  # Limit length
    }

    if traceback_str:
        # Only keep last 10 lines of traceback, sanitized
        tb_lines = traceback_str.strip().split('\n')[-10:]
        sanitized["traceback"] = [sanitize_string(line) for line in tb_lines]

    # Only include book info if error reporting settings allow it
    config = load_config()
    if book_info and config.get('error_reporting_include_titles', True):
        # Only include title/author, nothing else
        sanitized["book"] = {
            "title": book_info.get("title", "")[:100] if book_info.get("title") else None,
            "author": book_info.get("author", "")[:100] if book_info.get("author") else None,
        }

    return sanitized


def report_anonymous_error(error_msg: str, traceback_str: str = None, book_info: dict = None, context: str = None):
    """
    Report an error anonymously if the user has opted in.

    This stores error reports locally. In a future version, these could be
    sent to a central server to help identify common issues.

    Args:
        error_msg: The error message
        traceback_str: Optional traceback string
        book_info: Optional dict with 'title' and 'author' keys (only used if error is book-related)
        context: Optional context string (e.g., "layer_1_processing", "scan", "api_call")
    """
    try:
        config = load_config()
        if not config.get('anonymous_error_reporting', False):
            return  # User has not opted in

        # Sanitize the error data
        report = sanitize_error_data(error_msg, traceback_str, book_info)
        if context:
            report["context"] = context[:50]  # Limit context length

        # Load existing reports
        reports = []
        if ERROR_REPORTS_PATH.exists():
            try:
                with open(ERROR_REPORTS_PATH, 'r') as f:
                    reports = json.load(f)
            except:
                reports = []

        # Add new report (keep last 100 reports to avoid file bloat)
        reports.append(report)
        reports = reports[-100:]

        # Save reports
        with open(ERROR_REPORTS_PATH, 'w') as f:
            json.dump(reports, f, indent=2)

        logger.debug(f"Anonymous error report saved (context: {context})")

    except Exception as e:
        # Never let error reporting cause additional errors
        logger.debug(f"Failed to save error report: {e}")


def get_error_reports() -> list:
    """Get all stored error reports for the debug menu."""
    if ERROR_REPORTS_PATH.exists():
        try:
            with open(ERROR_REPORTS_PATH, 'r') as f:
                return json.load(f)
        except:
            return []
    return []


def clear_error_reports():
    """Clear all stored error reports."""
    if ERROR_REPORTS_PATH.exists():
        ERROR_REPORTS_PATH.unlink()


# Directory for permanent report storage (for developer review)
REPORTS_DIR = Path("/home/deucebucket/library-manager-reports")


def send_error_report_email(reports: list, user_message: str = None) -> dict:
    """
    Send error reports via email to the developer.
    Always saves locally first, then attempts email.

    Local storage is the primary method - email is best-effort.
    Returns success if local save works, even if email fails.
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from datetime import datetime

    # Create report content
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_id = f"report_{timestamp}"

    # Build report body
    body_parts = []
    body_parts.append(f"Error Report from Library Manager v{APP_VERSION}")
    body_parts.append(f"Submitted: {datetime.now().isoformat()}")
    body_parts.append(f"Report ID: {report_id}")
    body_parts.append("")

    if user_message:
        body_parts.append("=== User Message ===")
        body_parts.append(user_message)
        body_parts.append("")

    body_parts.append(f"=== {len(reports)} Error(s) ===")
    for i, report in enumerate(reports, 1):
        body_parts.append(f"\n--- Error {i} ---")
        body_parts.append(f"Time: {report.get('timestamp', 'Unknown')}")
        body_parts.append(f"Context: {report.get('context', 'Unknown')}")
        body_parts.append(f"Error: {report.get('error', 'No error message')}")
        if report.get('traceback'):
            body_parts.append(f"Traceback:\n{report['traceback']}")
        if report.get('book_info'):
            body_parts.append(f"Book: {report['book_info']}")

    report_body = "\n".join(body_parts)

    # Step 1: Save locally (required)
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        local_file = REPORTS_DIR / f"{report_id}.txt"
        with open(local_file, 'w') as f:
            f.write(report_body)
        logger.info(f"Error report saved locally: {local_file}")
    except Exception as e:
        logger.error(f"Failed to save error report locally: {e}")
        return {
            'success': False,
            'error': f'Failed to save report: {e}'
        }

    # Step 2: Try to send email (best-effort)
    email_sent = False
    email_error = None
    try:
        msg = MIMEMultipart()
        msg['From'] = 'library-manager@localhost'
        msg['To'] = 'lib-man-reports@deucebucket.com'
        msg['Subject'] = f'[Library Manager] Error Report - {len(reports)} error(s)'
        msg.attach(MIMEText(report_body, 'plain'))

        with smtplib.SMTP('localhost', 25) as server:
            server.send_message(msg)

        email_sent = True
        logger.info(f"Error report emailed: {report_id}")
    except Exception as e:
        email_error = str(e)
        logger.warning(f"Email send failed (report still saved locally): {e}")

    # Clear local error reports after successful save
    clear_error_reports()

    # Return success - local save is what matters
    if email_sent:
        return {
            'success': True,
            'message': f'Report saved and emailed (ID: {report_id})',
            'report_id': report_id,
            'email_sent': True
        }
    else:
        return {
            'success': True,
            'message': f'Report saved locally (ID: {report_id}). Email failed: {email_error}',
            'report_id': report_id,
            'email_sent': False,
            'email_error': email_error
        }


# ============== DRASTIC CHANGE DETECTION ==============

def is_drastic_author_change(old_author, new_author):
    """
    Check if an author change is "drastic" (completely different person)
    vs just formatting (case change, initials expanded, etc.)

    Returns True if the change is drastic and should require approval.
    """
    if not old_author or not new_author:
        return False

    # Normalize for comparison
    old_norm = old_author.lower().strip()
    new_norm = new_author.lower().strip()

    # Placeholder authors - going FROM these to a real author is NOT drastic
    placeholder_authors = {'unknown', 'various', 'various authors', 'va', 'n/a', 'none',
                           'audiobook', 'audiobooks', 'ebook', 'ebooks', 'book', 'books',
                           'author', 'authors', 'narrator', 'untitled', 'no author',
                           'metadata', 'tmp', 'temp', 'streams', 'cache'}  # System folders too
    if old_norm in placeholder_authors:
        return False  # Finding the real author is always good


    # If they're the same after normalization, not drastic
    if old_norm == new_norm:
        return False

    # Extract key words (remove common prefixes/suffixes)
    def get_name_parts(name):
        # Remove punctuation and split
        import re
        clean = re.sub(r'[^\w\s]', ' ', name.lower())
        parts = [p for p in clean.split() if len(p) > 1]
        return set(parts)

    old_parts = get_name_parts(old_author)
    new_parts = get_name_parts(new_author)

    # If no overlap at all, definitely drastic
    if not old_parts.intersection(new_parts):
        # Check for initials match (e.g., "J.R.R. Tolkien" vs "Tolkien")
        # Get last names (usually the longest word or last word)
        old_last = max(old_parts, key=len) if old_parts else ""
        new_last = max(new_parts, key=len) if new_parts else ""

        if old_last and new_last and (old_last in new_last or new_last in old_last):
            return False  # Probably same person

        return True  # Completely different

    # Some overlap - check how much
    overlap = len(old_parts.intersection(new_parts))
    total = max(len(old_parts), len(new_parts))

    # If less than 30% overlap, consider it drastic
    if total > 0 and overlap / total < 0.3:
        return True

    return False


def is_placeholder_author(name):
    """Check if an author name is a placeholder/system name that should be replaced."""
    if not name:
        return True
    name_lower = name.lower().strip()
    placeholder_authors = {'unknown', 'unknown author', 'various', 'various authors', 'va', 'n/a', 'none',
                           'audiobook', 'audiobooks', 'ebook', 'ebooks', 'book', 'books',
                           'author', 'authors', 'narrator', 'untitled', 'no author',
                           'metadata', 'tmp', 'temp', 'streams', 'cache', 'data', 'log', 'logs',
                           'audio', 'media', 'files', 'downloads', 'torrents'}
    return name_lower in placeholder_authors

# ============== LANGUAGE DETECTION ==============

def detect_title_language(text):
    """
    Detect the language of a title/text using langdetect.
    Returns ISO 639-1 language code or 'en' as fallback.
    """
    if not text or len(text.strip()) < 3:
        return 'en'  # Too short to detect

    try:
        from langdetect import detect, DetectorFactory
        # Make detection deterministic
        DetectorFactory.seed = 0
        detected = detect(text)
        logger.debug(f"Detected language '{detected}' for text: {text}")
        return detected
    except Exception as e:
        logger.debug(f"Language detection failed for '{text}': {e}")
        return 'en'  # Default to English on failure


def should_preserve_original_title(original_title, suggested_title, config):
    """
    Check if we should keep the original title instead of replacing it.

    This prevents replacing foreign language titles with English translations,
    e.g., "Der Bücherdrache" should NOT become "The Book Dragon" for German users.

    Returns True if original title should be preserved.
    """
    if not config.get('preserve_original_titles', True):
        return False

    if not original_title or not suggested_title:
        return False

    # If titles are the same, no preservation needed
    if original_title.strip().lower() == suggested_title.strip().lower():
        return False

    # Detect language of original title
    original_lang = detect_title_language(original_title)
    suggested_lang = detect_title_language(suggested_title)

    # Get user's preferred language
    preferred_lang = config.get('preferred_language', 'en')

    # If original is in user's preferred language but suggestion is in English,
    # preserve the original (user wants German, original is German, don't replace with English)
    if original_lang == preferred_lang and suggested_lang == 'en' and preferred_lang != 'en':
        logger.info(f"Preserving original title '{original_title}' ({original_lang}) - user prefers {preferred_lang}")
        return True

    # If original is in a non-English language and suggestion is English,
    # preserve original if "preserve_original_titles" is enabled
    if original_lang != 'en' and suggested_lang == 'en':
        logger.info(f"Preserving foreign title '{original_title}' ({original_lang}) instead of English '{suggested_title}'")
        return True

    return False


# Audible marketplace mappings for language preference
AUDIBLE_LANGUAGE_ENDPOINTS = {
    'en': 'audible.com',      # US/English
    'de': 'audible.de',       # German
    'fr': 'audible.fr',       # French
    'it': 'audible.it',       # Italian
    'es': 'audible.es',       # Spanish
    'jp': 'audible.co.jp',    # Japanese (ISO code is 'ja' but Audible uses 'jp')
    'ja': 'audible.co.jp',    # Japanese
    'au': 'audible.com.au',   # Australia (English)
    'uk': 'audible.co.uk',    # UK (English)
    'in': 'audible.in',       # India (English)
    'ca': 'audible.ca',       # Canada (English/French)
}


def get_audible_region_for_language(lang_code):
    """
    Get the appropriate Audible region code for a language.
    Audnexus uses these region codes in its API.
    """
    # Map language codes to Audnexus region codes
    region_map = {
        'en': 'us',   # Default English to US
        'de': 'de',   # German
        'fr': 'fr',   # French
        'it': 'it',   # Italian
        'es': 'es',   # Spanish
        'ja': 'jp',   # Japanese
        'pt': 'us',   # Portuguese -> US (no dedicated Audible)
        'nl': 'de',   # Dutch -> Germany (no dedicated Audible)
    }
    return region_map.get(lang_code, 'us')


# ISO 639-1 language code to full name mapping
LANGUAGE_NAMES = {
    'en': 'English', 'de': 'German', 'fr': 'French', 'es': 'Spanish',
    'it': 'Italian', 'pt': 'Portuguese', 'nl': 'Dutch', 'sv': 'Swedish',
    'no': 'Norwegian', 'da': 'Danish', 'fi': 'Finnish', 'pl': 'Polish',
    'ru': 'Russian', 'ja': 'Japanese', 'zh': 'Chinese', 'ko': 'Korean',
    'ar': 'Arabic', 'he': 'Hebrew', 'hi': 'Hindi', 'tr': 'Turkish',
    'cs': 'Czech', 'hu': 'Hungarian', 'el': 'Greek', 'th': 'Thai',
    'vi': 'Vietnamese', 'uk': 'Ukrainian', 'ro': 'Romanian', 'id': 'Indonesian'
}


def get_localized_title_via_ai(title, author, target_language, config):
    """
    Ask AI to find the official localized title of a book.
    This helps when APIs don't have results in the user's preferred language.

    Args:
        title: The book title (usually in English)
        author: The author name
        target_language: ISO 639-1 code (e.g., 'de' for German)
        config: App config with AI credentials

    Returns:
        dict with localized_title and is_translation, or None on failure
    """
    if not target_language or target_language == 'en':
        return None  # No translation needed

    lang_name = LANGUAGE_NAMES.get(target_language, target_language)

    prompt = f"""You are a book metadata expert with knowledge of international book translations.

Given this book:
- Title: {title}
- Author: {author}

What is the official {lang_name} ({target_language}) published title of this book?

RULES:
1. If this book has an official {lang_name} translation, return that title
2. If the book was originally written in {lang_name}, return the original title
3. If no official translation exists, return the original title
4. DO NOT invent translations - only use real published titles

Return JSON only:
{{"localized_title": "the {lang_name} title", "is_translation": true/false, "notes": "brief explanation"}}

If you're not certain, return: {{"localized_title": "{title}", "is_translation": false, "notes": "no official translation found"}}"""

    provider = config.get('ai_provider', 'openrouter')

    try:
        if provider == 'ollama':
            result = _call_ollama_simple(prompt, config)
        elif provider == 'gemini' and config.get('gemini_api_key'):
            result = _call_gemini_simple(prompt, config)
        elif config.get('openrouter_api_key'):
            result = _call_openrouter_simple(prompt, config)
        else:
            return None

        if result and result.get('localized_title'):
            logger.info(f"AI localization: '{title}' -> '{result['localized_title']}' ({lang_name})")
            return result
    except Exception as e:
        logger.debug(f"AI localization failed: {e}")

    return None


def _call_openrouter_simple(prompt, config):
    """Simple OpenRouter call for localization queries."""
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {config['openrouter_api_key']}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.get('openrouter_model', 'google/gemma-3n-e4b-it:free'),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1
            },
            timeout=30
        )
        if resp.status_code == 200:
            text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            return parse_json_response(text) if text else None
    except Exception as e:
        logger.debug(f"OpenRouter localization error: {e}")
    return None


def _call_gemini_simple(prompt, config):
    """Simple Gemini call for localization queries."""
    try:
        api_key = config.get('gemini_api_key')
        model = config.get('gemini_model', 'gemini-2.0-flash')
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=30
        )
        if resp.status_code == 200:
            text = resp.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            return parse_json_response(text) if text else None
    except Exception as e:
        logger.debug(f"Gemini localization error: {e}")
    return None


def _call_ollama_simple(prompt, config):
    """Simple Ollama call for localization queries."""
    try:
        ollama_url = config.get('ollama_url', 'http://localhost:11434')
        model = config.get('ollama_model', 'llama3.2:3b')
        resp = requests.post(
            f"{ollama_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1}
            },
            timeout=60
        )
        if resp.status_code == 200:
            text = resp.json().get("response", "")
            return parse_json_response(text) if text else None
    except Exception as e:
        logger.debug(f"Ollama localization error: {e}")
    return None


# ============== BOOK METADATA APIs ==============

# Rate limiting for each API (last call timestamp)
# Based on research:
# - Audnexus: No docs, small project - 1 req/sec max
# - OpenLibrary: Had issues with high traffic - 1 req/sec
# - Google Books: ~1000/day free = ~40/hour - 1 req/2sec
# - Hardcover: Beta API, be conservative - 1 req/2sec
API_RATE_LIMITS = {
    'audnexus': {'last_call': 0, 'min_delay': 1.5},      # 1.5 sec between calls
    'openlibrary': {'last_call': 0, 'min_delay': 1.5},   # 1.5 sec between calls
    'googlebooks': {'last_call': 0, 'min_delay': 2.5},   # 2.5 sec between calls (stricter)
    'hardcover': {'last_call': 0, 'min_delay': 2.5},     # 2.5 sec between calls (beta)
}
API_RATE_LOCK = threading.Lock()

def rate_limit_wait(api_name):
    """Wait if needed to respect rate limits for the given API."""
    with API_RATE_LOCK:
        if api_name not in API_RATE_LIMITS:
            return

        limit_info = API_RATE_LIMITS[api_name]
        now = time.time()
        elapsed = now - limit_info['last_call']
        wait_time = limit_info['min_delay'] - elapsed

        if wait_time > 0:
            logger.debug(f"Rate limiting {api_name}: waiting {wait_time:.1f}s")
            time.sleep(wait_time)

        API_RATE_LIMITS[api_name]['last_call'] = time.time()


def sanitize_path_component(name):
    """Sanitize a path component to prevent directory traversal and invalid chars.

    CRITICAL SAFETY FUNCTION - prevents catastrophic file moves.
    """
    if not name or not isinstance(name, str):
        return None

    # Strip whitespace
    name = name.strip()

    # Block empty strings
    if not name:
        return None

    # Block directory traversal attempts
    if '..' in name or name.startswith('/') or name.startswith('\\'):
        logger.warning(f"BLOCKED dangerous path component: {name}")
        return None

    # Remove/replace dangerous characters
    # Windows: < > : " / \ | ? *
    # Also remove control characters
    dangerous_chars = '<>:"/\\|?*\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
    for char in dangerous_chars:
        name = name.replace(char, '')

    # Final strip and check
    name = name.strip('. ')  # Windows doesn't like trailing dots/spaces
    if not name or len(name) < 2:
        return None

    return name


def build_new_path(lib_path, author, title, series=None, series_num=None, narrator=None, year=None,
                   edition=None, variant=None, config=None):
    """Build a new path based on the naming format configuration.

    Audiobookshelf-compatible format (when series_grouping enabled):
    - Narrator in curly braces: {Ray Porter}
    - Series number prefix: "1 - Title"
    - Year in parentheses: (2003)
    - Edition in brackets: [30th Anniversary Edition]
    - Variant in brackets: [Graphic Audio]

    SAFETY: Returns None if path would be invalid/dangerous.
    """
    naming_format = config.get('naming_format', 'author/title') if config else 'author/title'
    series_grouping = config.get('series_grouping', False) if config else False

    # CRITICAL SAFETY: Sanitize all path components
    safe_author = sanitize_path_component(author)
    safe_title = sanitize_path_component(title)
    safe_series = sanitize_path_component(series) if series else None

    # CRITICAL: Reject if author or title are invalid
    if not safe_author or not safe_title:
        logger.error(f"BLOCKED: Invalid author '{author}' or title '{title}' - would create dangerous path")
        return None

    # Build title folder name
    title_folder = safe_title

    # Add series number prefix if series grouping enabled and we have series info
    if series_grouping and safe_series and series_num:
        title_folder = f"{series_num} - {safe_title}"

    # Add edition/variant in brackets (e.g., [30th Anniversary Edition], [Graphic Audio])
    # These distinguish different versions of the same book
    if variant:
        safe_variant = sanitize_path_component(variant)
        if safe_variant:
            title_folder = f"{title_folder} [{safe_variant}]"
    elif edition:
        safe_edition = sanitize_path_component(edition)
        if safe_edition:
            title_folder = f"{title_folder} [{safe_edition}]"

    # Add year if present (and no edition/variant already added for version distinction)
    if year and not edition and not variant:
        title_folder = f"{title_folder} ({year})"

    # Add narrator - curly braces for ABS format, parentheses otherwise
    if narrator:
        safe_narrator = sanitize_path_component(narrator)
        if safe_narrator:
            if series_grouping:
                # ABS format uses curly braces for narrator
                title_folder = f"{title_folder} {{{safe_narrator}}}"
            else:
                # Legacy format uses parentheses
                title_folder = f"{title_folder} ({safe_narrator})"

    if naming_format == 'custom':
        # Custom template: parse and replace tags
        custom_template = config.get('custom_naming_template', '{author}/{title}') if config else '{author}/{title}'

        # Prepare all available data for replacement
        safe_narrator = sanitize_path_component(narrator) if narrator else ''
        safe_year = str(year) if year else ''
        safe_edition = sanitize_path_component(edition) if edition else ''
        safe_variant = sanitize_path_component(variant) if variant else ''
        safe_series_num = str(series_num) if series_num else ''

        # Build the path from template
        path_str = custom_template
        path_str = path_str.replace('{author}', safe_author)
        path_str = path_str.replace('{title}', safe_title)
        path_str = path_str.replace('{series}', safe_series or '')
        path_str = path_str.replace('{series_num}', safe_series_num)
        path_str = path_str.replace('{narrator}', safe_narrator)
        path_str = path_str.replace('{year}', safe_year)
        path_str = path_str.replace('{edition}', safe_edition)
        path_str = path_str.replace('{variant}', safe_variant)

        # Clean up empty brackets/parens from missing optional data
        import re
        path_str = re.sub(r'\(\s*\)', '', path_str)  # Empty ()
        path_str = re.sub(r'\[\s*\]', '', path_str)  # Empty []
        path_str = re.sub(r'\{\s*\}', '', path_str)  # Empty {} (literal, not tags)
        path_str = re.sub(r'\s+-\s+(?=-|/|$)', '', path_str)  # Dangling " - " before separator
        path_str = re.sub(r'/\s*-\s+', '/', path_str)  # Leading "- " or " - " after slash (Issue #16, #22)
        path_str = re.sub(r'^-\s+', '', path_str)  # Leading "- " at start
        path_str = re.sub(r'^\s*-\s+', '', path_str)  # Leading " - " at start (with space)
        path_str = re.sub(r'\s+-$', '', path_str)  # Trailing " -" at end
        path_str = re.sub(r'/+', '/', path_str)  # Multiple slashes
        path_str = re.sub(r'\s{2,}', ' ', path_str)  # Multiple spaces
        path_str = path_str.strip(' /')

        # Split by / to create path components
        parts = [p.strip() for p in path_str.split('/') if p.strip()]
        if not parts:
            logger.error(f"BLOCKED: Custom template resulted in empty path")
            return None

        result_path = lib_path
        for part in parts:
            result_path = result_path / part
    elif naming_format == 'author - title':
        # Flat structure: Author - Title (single folder)
        folder_name = f"{safe_author} - {title_folder}"
        result_path = lib_path / folder_name
    elif series_grouping and safe_series:
        # Series grouping enabled AND book has series: Author/Series/Title
        result_path = lib_path / safe_author / safe_series / title_folder
    else:
        # Default: Author/Title (two-level)
        result_path = lib_path / safe_author / title_folder

    # CRITICAL SAFETY: Verify path is within library and has minimum depth
    try:
        # Resolve to absolute path
        result_path = result_path.resolve()
        lib_path_resolved = Path(lib_path).resolve()

        # Ensure result is within library path
        result_path.relative_to(lib_path_resolved)

        # Ensure minimum depth (at least 1 folder below library root)
        relative = result_path.relative_to(lib_path_resolved)
        if len(relative.parts) < 1:
            logger.error(f"BLOCKED: Path too shallow - would dump files at library root: {result_path}")
            return None

    except ValueError:
        logger.error(f"BLOCKED: Path escapes library! lib={lib_path}, result={result_path}")
        return None

    return result_path


def clean_search_title(messy_name):
    """Clean up a messy filename to extract searchable title."""
    import re
    # Remove common junk patterns
    clean = messy_name

    # Convert underscores to spaces first (common in filenames)
    clean = clean.replace('_', ' ')

    # Remove bracketed content like [bitsearch.to], [64k], [r1.1]
    clean = re.sub(r'\[.*?\]', '', clean)
    # Remove parenthetical junk like (Unabridged), (2019) - but keep series info like (Book 1)
    clean = re.sub(r'\((?:Unabridged|Abridged|MP3|M4B|EPUB|PDF|64k|128k|r\d+\.\d+|multi).*?\)', '', clean, flags=re.IGNORECASE)
    # Remove curly brace junk like {465mb}, {narrator}
    clean = re.sub(r'\{[^}]*(?:mb|kb|kbps|narrator|reader)[^}]*\}', '', clean, flags=re.IGNORECASE)
    # Remove file extensions
    clean = re.sub(r'\.(mp3|m4b|m4a|epub|pdf|mobi|webm|opus)$', '', clean, flags=re.IGNORECASE)
    # Remove "by Author" at the end temporarily for searching
    clean = re.sub(r'\s+by\s+[\w\s]+$', '', clean, flags=re.IGNORECASE)
    # Remove audiobook-related junk (YouTube rip artifacts)
    clean = re.sub(r'\b(full\s+)?audiobook\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(complete|unabridged|abridged)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(audio\s*book|audio)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(free|download|hd|hq)\b', '', clean, flags=re.IGNORECASE)
    # NOTE: We intentionally DON'T strip date/timestamp patterns here
    # Books like "11/22/63" by Stephen King have dates AS titles
    # The layered verification system (API + AI + Audio) will determine the real title
    # This function just cleans obvious junk for searching - title verification is separate

    # Strip leading track/chapter numbers like "06 - Title", "01. Title", "Track 05 - Title"
    # These are common in audiobook folders but mess up search
    clean = re.sub(r'^(?:track\s*)?\d+\s*[-–—:.]\s*', '', clean, flags=re.IGNORECASE)

    # Remove extra whitespace
    clean = re.sub(r'\s+', ' ', clean)
    # Remove leading/trailing junk
    clean = clean.strip(' -_.')
    return clean


# BookDB API endpoint (our private metadata service)
BOOKDB_API_URL = "https://bookdb.deucebucket.com"

def search_bookdb(title, author=None, api_key=None):
    """
    Search our private BookDB metadata service.
    Uses fuzzy matching via Qdrant vectors - great for messy filenames.
    Returns series info including book position if found.
    """
    if not api_key:
        return None

    try:
        # Build the filename to match - include author if we have it
        filename = f"{author} - {title}" if author else title

        resp = requests.post(
            f"{BOOKDB_API_URL}/match",
            json={"filename": filename},
            headers={"X-API-Key": api_key},
            timeout=10
        )

        if resp.status_code != 200:
            logger.debug(f"BookDB returned status {resp.status_code}")
            return None

        data = resp.json()

        # Check confidence threshold
        if data.get('confidence', 0) < 0.5:
            logger.debug(f"BookDB match below confidence threshold: {data.get('confidence')}")
            return None

        series = data.get('series')
        books = data.get('books', [])

        if not series:
            return None

        # Find the best matching book in the series
        best_book = None
        if books:
            # Try to match title to a specific book in series
            title_lower = title.lower()
            for book in books:
                book_title = book.get('title', '').lower()
                if title_lower in book_title or book_title in title_lower:
                    best_book = book
                    break
            # If no specific match, use first book
            if not best_book:
                best_book = books[0]

        result = {
            'title': best_book.get('title') if best_book else series.get('name'),
            'author': series.get('author_name', ''),
            'year': best_book.get('year_published') if best_book else None,
            'series': series.get('name'),
            'series_num': best_book.get('series_position') if best_book else None,
            'variant': series.get('variant'),  # Graphic Audio, BBC Radio, etc.
            'edition': best_book.get('edition') if best_book else None,
            'source': 'bookdb',
            'confidence': data.get('confidence', 0)
        }

        if result['title'] and result['author']:
            logger.info(f"BookDB found: {result['author']} - {result['title']}" +
                       (f" ({result['series']} #{result['series_num']})" if result['series'] else "") +
                       f" [confidence: {result['confidence']:.2f}]")
            return result
        return None

    except Exception as e:
        logger.debug(f"BookDB search failed: {e}")
        return None


def search_openlibrary(title, author=None, lang=None):
    """Search OpenLibrary for book metadata. Free, no API key needed.

    Args:
        title: Book title to search for
        author: Optional author name
        lang: Optional ISO 639-1 language code to filter results
    """
    rate_limit_wait('openlibrary')
    try:
        import urllib.parse
        query = urllib.parse.quote(title)
        url = f"https://openlibrary.org/search.json?title={query}&limit=5"
        if author:
            url += f"&author={urllib.parse.quote(author)}"
        if lang and lang != 'en':
            # OpenLibrary supports language filtering
            url += f"&language={lang}"

        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        docs = data.get('docs', [])

        if not docs:
            return None

        # Get the best match (first result usually best)
        best = docs[0]
        result = {
            'title': best.get('title', ''),
            'author': best.get('author_name', [''])[0] if best.get('author_name') else '',
            'year': best.get('first_publish_year'),
            'source': 'openlibrary'
        }

        # Only return if we got useful data
        if result['title'] and result['author']:
            logger.info(f"OpenLibrary found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"OpenLibrary search failed: {e}")
        return None

def search_google_books(title, author=None, api_key=None, lang=None):
    """Search Google Books for book metadata.

    Args:
        title: Book title to search for
        author: Optional author name
        api_key: Optional Google API key for higher rate limits
        lang: Optional ISO 639-1 language code to restrict results (e.g., 'de' for German)
    """
    rate_limit_wait('googlebooks')
    try:
        import urllib.parse
        query = title
        if author:
            query += f" inauthor:{author}"

        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=5"
        if api_key:
            url += f"&key={api_key}"
        if lang and lang != 'en':
            # langRestrict filters results to books in this language
            url += f"&langRestrict={lang}"

        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        items = data.get('items', [])

        if not items:
            return None

        # Get best match
        best = items[0].get('volumeInfo', {})
        authors = best.get('authors', [])

        # Try to extract series from subtitle (e.g., "A Mistborn Novel", "Book 2 of The Expanse")
        series_name = None
        series_num = None
        subtitle = best.get('subtitle', '')
        if subtitle:
            # "A Mistborn Novel" -> Mistborn
            match = re.search(r'^A\s+(.+?)\s+Novel$', subtitle, re.IGNORECASE)
            if match:
                series_name = match.group(1)
            # "Book 2 of The Expanse" -> The Expanse, 2
            match = re.search(r'Book\s+(\d+)\s+of\s+(.+)', subtitle, re.IGNORECASE)
            if match:
                series_num = int(match.group(1))
                series_name = match.group(2)
            # "The Expanse Book 2" or "Mistborn #1"
            match = re.search(r'(.+?)\s+(?:Book|#)\s*(\d+)', subtitle, re.IGNORECASE)
            if match:
                series_name = match.group(1)
                series_num = int(match.group(2))

        result = {
            'title': best.get('title', ''),
            'author': authors[0] if authors else '',
            'year': best.get('publishedDate', '')[:4] if best.get('publishedDate') else None,
            'series': series_name,
            'series_num': series_num,
            'source': 'googlebooks'
        }

        if result['title'] and result['author']:
            logger.info(f"Google Books found: {result['author']} - {result['title']}" +
                       (f" (Series: {series_name})" if series_name else ""))
            return result
        return None
    except Exception as e:
        logger.debug(f"Google Books search failed: {e}")
        return None

def search_audnexus(title, author=None, region=None):
    """Search Audnexus API for audiobook metadata. Pulls from Audible.

    Args:
        title: Book title to search for
        author: Optional author name
        region: Optional Audible region code (us, de, fr, it, es, jp, etc.)
    """
    rate_limit_wait('audnexus')
    try:
        import urllib.parse
        # Audnexus search endpoint
        query = title
        if author:
            query = f"{title} {author}"

        url = f"https://api.audnex.us/books?title={urllib.parse.quote(query)}"
        # Add region parameter for localized results
        if region and region != 'us':
            url += f"&region={region}"

        resp = requests.get(url, timeout=10, headers={'Accept': 'application/json'})
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        # Get best match
        best = data[0]
        result = {
            'title': best.get('title', ''),
            'author': best.get('authors', [{}])[0].get('name', '') if best.get('authors') else '',
            'year': best.get('releaseDate', '')[:4] if best.get('releaseDate') else None,
            'narrator': best.get('narrators', [{}])[0].get('name', '') if best.get('narrators') else None,
            'source': 'audnexus'
        }

        if result['title'] and result['author']:
            logger.info(f"Audnexus found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"Audnexus search failed: {e}")
        return None

def search_hardcover(title, author=None):
    """Search Hardcover.app API for book metadata."""
    rate_limit_wait('hardcover')
    try:
        import urllib.parse
        # Hardcover GraphQL API
        query = title
        if author:
            query = f"{title} {author}"

        # Hardcover uses GraphQL
        graphql_query = {
            "query": """
                query SearchBooks($query: String!) {
                    search(query: $query, limit: 5) {
                        books {
                            title
                            contributions { author { name } }
                            releaseYear
                        }
                    }
                }
            """,
            "variables": {"query": query}
        }

        resp = requests.post(
            "https://api.hardcover.app/v1/graphql",
            json=graphql_query,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )

        if resp.status_code != 200:
            return None

        data = resp.json()
        books = data.get('data', {}).get('search', {}).get('books', [])

        if not books:
            return None

        best = books[0]
        contributions = best.get('contributions', [])
        author_name = contributions[0].get('author', {}).get('name', '') if contributions else ''

        result = {
            'title': best.get('title', ''),
            'author': author_name,
            'year': best.get('releaseYear'),
            'source': 'hardcover'
        }

        if result['title'] and result['author']:
            logger.info(f"Hardcover found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"Hardcover search failed: {e}")
        return None

def extract_author_title(messy_name):
    """Try to extract author and title from a folder name like 'Author - Title' or 'Author/Title'."""
    import re

    # Common separators: " - ", " / ", " _ "
    separators = [' - ', ' / ', ' _ ', ' – ']  # includes en-dash

    for sep in separators:
        if sep in messy_name:
            parts = messy_name.split(sep, 1)
            if len(parts) == 2:
                author = parts[0].strip()
                title = parts[1].strip()
                # Basic validation - author shouldn't be too long or look like a title
                if len(author) < 50 and not re.search(r'\d{4}|book|vol|part|\[', author, re.I):
                    return author, title

    # No separator found - just return the whole thing as title
    return None, messy_name

def lookup_book_metadata(messy_name, config, folder_path=None):
    """Try to look up book metadata from multiple APIs, cycling through until found.
    Now with garbage match filtering and folder metadata extraction."""
    # Try to extract author and title separately for better search
    author_hint, title_part = extract_author_title(messy_name)
    clean_title = clean_search_title(title_part)

    if not clean_title or len(clean_title) < 3:
        return None

    # Extract metadata from folder files if path provided
    folder_hints = {}
    if folder_path:
        folder_hints = extract_folder_metadata(folder_path)
        if folder_hints:
            logger.debug(f"Found folder metadata hints: {folder_hints}")
            # Use folder metadata as additional hints
            if 'audio_author' in folder_hints and not author_hint:
                author_hint = folder_hints['audio_author']
            if 'audio_title' in folder_hints:
                # Prefer audio metadata title if clean_title looks like garbage
                if len(clean_title) < 5 or clean_title.lower().startswith('chapter'):
                    clean_title = folder_hints['audio_title']

    if author_hint:
        logger.debug(f"Looking up metadata for: '{clean_title}' by '{author_hint}'")
    else:
        logger.debug(f"Looking up metadata for: {clean_title}")

    def validate_result(result, original_title):
        """Check if API result is a garbage match."""
        if not result:
            return None
        suggested_title = result.get('title', '')
        if is_garbage_match(original_title, suggested_title):
            logger.info(f"REJECTED garbage match: '{original_title}' -> '{suggested_title}'")
            return None
        return result

    # Get language preference settings
    preferred_lang = config.get('preferred_language', 'en')
    audible_region = get_audible_region_for_language(preferred_lang)

    # 0. Try BookDB first (our private metadata service with fuzzy matching)
    bookdb_key = config.get('bookdb_api_key')
    if bookdb_key:
        result = validate_result(search_bookdb(clean_title, author=author_hint, api_key=bookdb_key), clean_title)
        if result:
            return result

    # 1. Try Audnexus (best for audiobooks, pulls from Audible)
    result = validate_result(search_audnexus(clean_title, author=author_hint, region=audible_region), clean_title)
    if result:
        return result

    # 2. Try OpenLibrary (free, huge database)
    result = validate_result(search_openlibrary(clean_title, author=author_hint, lang=preferred_lang), clean_title)
    if result:
        return result

    # 3. Try Google Books
    google_key = config.get('google_books_api_key')
    result = validate_result(search_google_books(clean_title, author=author_hint, api_key=google_key, lang=preferred_lang), clean_title)
    if result:
        return result

    # 4. Try Hardcover.app (modern Goodreads alternative)
    result = validate_result(search_hardcover(clean_title, author=author_hint), clean_title)
    if result:
        return result

    logger.debug(f"No valid API results for: {clean_title}")
    return None


def gather_all_api_candidates(title, author=None, config=None):
    """
    Search ALL APIs and return ALL results (not just the first match).
    This is used for verification when we need multiple perspectives.
    Now with garbage match filtering and language preference support.
    """
    candidates = []
    clean_title = clean_search_title(title)

    if not clean_title or len(clean_title) < 3:
        return candidates

    # Get language preference settings
    preferred_lang = config.get('preferred_language', 'en') if config else 'en'
    audible_region = get_audible_region_for_language(preferred_lang)

    # Search each API and collect all results
    apis = [
        ('BookDB', lambda t, a: search_bookdb(t, a, config.get('bookdb_api_key') if config else None)),
        ('Audnexus', lambda t, a: search_audnexus(t, a, region=audible_region)),
        ('OpenLibrary', lambda t, a: search_openlibrary(t, a, lang=preferred_lang)),
        ('GoogleBooks', lambda t, a: search_google_books(t, a, config.get('google_books_api_key') if config else None, lang=preferred_lang)),
        ('Hardcover', search_hardcover),
    ]

    for api_name, search_func in apis:
        try:
            # Search with author hint
            result = search_func(clean_title, author)
            if result:
                # Filter garbage matches
                suggested_title = result.get('title', '')
                if is_garbage_match(clean_title, suggested_title):
                    logger.debug(f"REJECTED garbage from {api_name}: '{clean_title}' -> '{suggested_title}'")
                else:
                    # Ensure source attribution for Book Profile system
                    result['source'] = api_name.lower()
                    result['search_query'] = f"{author} - {clean_title}" if author else clean_title
                    candidates.append(result)

            # Also search without author (might find different results)
            if author:
                result_no_author = search_func(clean_title, None)
                if result_no_author:
                    suggested_title = result_no_author.get('title', '')
                    if is_garbage_match(clean_title, suggested_title):
                        logger.debug(f"REJECTED garbage from {api_name}: '{clean_title}' -> '{suggested_title}'")
                    elif result_no_author.get('author') != (result.get('author') if result else None):
                        # Ensure source attribution for Book Profile system
                        result_no_author['source'] = api_name.lower()
                        result_no_author['search_query'] = clean_title
                        candidates.append(result_no_author)
        except Exception as e:
            logger.debug(f"Error searching {api_name}: {e}")

    # Deduplicate by author+title
    seen = set()
    unique_candidates = []
    for c in candidates:
        key = f"{c.get('author', '').lower()}|{c.get('title', '').lower()}"
        if key not in seen:
            seen.add(key)
            unique_candidates.append(c)

    return unique_candidates


def build_verification_prompt(original_input, original_author, original_title, proposed_author, proposed_title, candidates):
    """
    Build a verification prompt that shows ALL API candidates and asks AI to vote.
    """
    candidate_list = ""
    for i, c in enumerate(candidates, 1):
        candidate_list += f"  CANDIDATE_{i}: {c.get('author', 'Unknown')} - {c.get('title', 'Unknown')} (from {c.get('source', 'Unknown')})\n"

    if not candidate_list:
        candidate_list = "  No API results found.\n"

    return f"""You are a book metadata verification expert. A drastic author change was detected and needs your verification.

ORIGINAL INPUT: {original_input}
  - Current Author: {original_author}
  - Current Title: {original_title}

PROPOSED CHANGE:
  - New Author: {proposed_author}
  - New Title: {proposed_title}

ALL API SEARCH RESULTS:
{candidate_list}

CRITICAL RULE - REJECT GARBAGE MATCHES:
The API sometimes returns COMPLETELY UNRELATED books that share one word. These are ALWAYS WRONG:
- "Chapter 19" -> "College Accounting, Chapters 1-9" = WRONG (different book!)
- "Death Genesis" -> "The Darkborn AfterLife Genesis" = WRONG (matching on "genesis" only)
- "Mr. Murder" -> "Frankenstein" = WRONG (no title overlap at all!)
- "Mortal Coils" -> "The Life and Letters of Thomas Huxley" = WRONG (completely different book)

If the proposed title shares LESS THAN HALF of its significant words with the original title, it is WRONG.

YOUR TASK:
Analyze whether the proposed change is CORRECT or WRONG. Consider:

1. TITLE MATCHING FIRST - Is this even the same book?
   - At least 50% of significant words must match
   - "Mr. Murder" and "Dean Koontz's Frankenstein" = WRONG (0% match!)
   - "Midnight Texas 3" and "Night Shift" = CORRECT if Night Shift is book 3 of Midnight Texas

2. AUTHOR MATCHING: Does the original author name match or partially match any candidate?
   - "Boyett" matches "Steven Boyett" (same person, use full name)
   - "Boyett" does NOT match "John Dickson Carr" (different person!)
   - "A.C. Crispin" matches "A. C. Crispin" or "Ann C. Crispin" (same person)

3. TRUST THE INPUT: If original has a real author name, KEEP that author unless clearly wrong.

4. FIND THE BEST MATCH: Pick the candidate whose author MATCHES or EXTENDS the original.

RESPOND WITH JSON ONLY:
{{
  "decision": "CORRECT" or "WRONG" or "UNCERTAIN",
  "recommended_author": "The correct author name",
  "recommended_title": "The correct title",
  "reasoning": "Brief explanation of why",
  "confidence": "HIGH" or "MEDIUM" or "LOW"
}}

DECISION RULES:
- If titles are completely different books = WRONG (don't just keyword match!)
- If original author matches a candidate (like Boyett -> Steven Boyett) = CORRECT
- If proposed author is completely different person AND same title = WRONG
- If uncertain = UNCERTAIN

When in doubt, say WRONG. It's better to leave a book unfixed than to rename it to the wrong thing."""


def verify_drastic_change(original_input, original_author, original_title, proposed_author, proposed_title, config):
    """
    Verify a drastic change by gathering all API candidates and having AI vote.
    Returns: {'verified': bool, 'author': str, 'title': str, 'reasoning': str}
    """
    logger.info(f"Verifying drastic change: {original_author} -> {proposed_author}")

    # Gather ALL candidates from ALL APIs
    candidates = gather_all_api_candidates(original_title, original_author, config)

    # Also search with proposed info to get more candidates
    if proposed_author and proposed_author != original_author:
        more_candidates = gather_all_api_candidates(proposed_title, proposed_author, config)
        for c in more_candidates:
            if c not in candidates:
                candidates.append(c)

    logger.info(f"Gathered {len(candidates)} candidates for verification")

    # Build verification prompt
    prompt = build_verification_prompt(
        original_input, original_author, original_title,
        proposed_author, proposed_title, candidates
    )

    # Call AI for verification
    provider = config.get('ai_provider', 'openrouter')
    try:
        if provider == 'gemini' and config.get('gemini_api_key'):
            verification = call_gemini(prompt, config)  # Already returns parsed dict
        elif config.get('openrouter_api_key'):
            verification = call_openrouter(prompt, config)  # Already returns parsed dict
        else:
            logger.error("No API key for verification!")
            return None

        if not verification:
            return None

        # Result is already parsed by call_gemini/call_openrouter

        decision = verification.get('decision', 'UNCERTAIN')
        confidence = verification.get('confidence', 'LOW')

        logger.info(f"Verification result: {decision} ({confidence}): {verification.get('reasoning', '')[:100]}")

        return {
            'verified': decision in ['CORRECT'] and confidence in ['HIGH', 'MEDIUM'],
            'decision': decision,
            'author': verification.get('recommended_author', original_author),
            'title': verification.get('recommended_title', original_title),
            'reasoning': verification.get('reasoning', ''),
            'confidence': confidence,
            'candidates_found': len(candidates)
        }
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return None


# ============== AI API ==============

def build_prompt(messy_names, api_results=None):
    """Build the parsing prompt for AI, including any API lookup results."""
    items = []
    for i, name in enumerate(messy_names):
        item_text = f"ITEM_{i+1}: {name}"
        # Add API lookup result if available
        if api_results and i < len(api_results) and api_results[i]:
            result = api_results[i]
            item_text += f"\n  -> API found: {result['author']} - {result['title']} (from {result['source']})"
        items.append(item_text)
    names_list = "\n".join(items)

    return f"""You are a book metadata expert. For each filename, identify the REAL author and title.

{names_list}

MOST IMPORTANT RULE - TRUST THE EXISTING AUTHOR:
If the input is already in "Author / Title" or "Author - Title" format with a human name as author:
- KEEP THAT AUTHOR unless you're 100% certain it's wrong
- Many books have the SAME TITLE by DIFFERENT AUTHORS
- Example: "The Hollow Man" exists by BOTH Steven Boyett AND John Dickson Carr - different books!
- Example: "Yellow" by Aron Beauregard is NOT "The King in Yellow" by Chambers!
- If API returns a DIFFERENT AUTHOR for the same title, TRUST THE INPUT AUTHOR

WHEN TO CHANGE THE AUTHOR:
- Only if the "author" in input is clearly NOT an author name (e.g., "Bastards Series", "Unknown", "Various")
- Only if the author/title are swapped (e.g., "Mistborn / Brandon Sanderson" -> swap them)
- Only if it's clearly gibberish

WHEN TO KEEP THE AUTHOR:
- Input: "Boyett/The Hollow Man" -> Keep author "Boyett" (Steven Boyett wrote this book!)
- Input: "Aron Beauregard/Yellow" -> Keep author "Aron Beauregard" (he wrote "Yellow"!)
- If it looks like a human name (First Last, or Last name), it's probably correct

API RESULTS WARNING - CRITICAL:
- API may return COMPLETELY WRONG books that share only one keyword!
- "Chapter 19" -> "College Accounting" = WRONG (API matched on "chapter" - garbage!)
- "Death Genesis" -> "The Darkborn AfterLife" = WRONG (API matched on "genesis" - garbage!)
- "Mr. Murder" -> "Frankenstein" = WRONG (no title match at all!)
- If API title is COMPLETELY DIFFERENT from input title, IGNORE THE API RESULT
- Same title can exist by different authors - if API author differs, keep INPUT author
- Only use API if input has NO author OR the titles closely match

LANGUAGE/CHARACTER RULES:
- ALWAYS use Latin/English characters for author and title names
- If input is "Dmitry Glukhovsky", output "Dmitry Glukhovsky" (NOT "Дмитрий Глуховский" in Cyrillic)
- If API returns non-Latin characters (Cyrillic, Chinese, etc.), convert to the Latin equivalent
- Keep the library consistent - English alphabet only

OTHER RULES:
- NEVER put "Book 1", "Book 2", etc. in the title field - that goes in series_num
- The title should be the ACTUAL book title, not "Series Name Book N"
- Remove junk: [bitsearch.to], [64k], version numbers, format tags, bitrates, file sizes
- Fix obvious typos in author names (e.g., "Annie Jacobson" -> "Annie Jacobsen")
- Clean up title formatting but PRESERVE the actual title - don't replace it

NARRATOR PRESERVATION (CRITICAL FOR AUDIOBOOKS):
- Parentheses at the END containing a SINGLE PROPER NAME (surname) = NARRATOR
- ONLY extract as narrator if it looks like a person's name (capitalized surname)
- Examples that ARE narrators: "(Kafer)", "(Palmer)", "(Vance)", "(Barker)", "(Glover)", "(Fry)", "(Brick)"

NOT NARRATORS - these are junk to REMOVE:
- Genres: "(Horror)", "(Sci-Fi)", "(Fantasy)", "(Romance)", "(Thriller)", "(Mystery)"
- Formats: "(Unabridged)", "(Abridged)", "(MP3)", "(M4B)", "(Audiobook)", "(AB)"
- Years: "(2020)", "(1985)", any 4-digit number
- Quality: "(64k)", "(128k)", "(HQ)", "(Complete)"
- Sources: "(Audible)", "(Librivox)", "(BBC)"
- Descriptors: "(Complete)", "(Full)", "(Retail)", "(SET)"

HOW TO TELL THE DIFFERENCE:
- Narrator = single capitalized word that looks like a surname (Vance, Brick, Fry)
- NOT narrator = common English words, genres, formats, numbers
- When in doubt, set narrator to null (don't guess)

SERIES DETECTION:
- If the book is part of a known series, set "series" to the series name and "series_num" to the book number
- The "title" field should be the ACTUAL BOOK TITLE - never "Series Book N"
- Examples of series books:
  - "Mistborn Book 1" -> series: "Mistborn", series_num: 1, title: "The Final Empire" (NOT "Mistborn Book 1")
  - "Dark One: The Forgotten" -> series: "Dark One", series_num: 2, title: "The Forgotten" (keep actual subtitle!)
  - "The Reckoners Book 2 - Firefight" -> series: "The Reckoners", series_num: 2, title: "Firefight"
  - "Eragon" -> series: "Inheritance Cycle", series_num: 1, title: "Eragon"
  - "The Eye of the World" -> series: "The Wheel of Time", series_num: 1, title: "The Eye of the World"
  - "Leviathan Wakes" -> series: "The Expanse", series_num: 1, title: "Leviathan Wakes"
- Standalone books (NOT in a series) -> series: null, series_num: null
  - "The Martian" by Andy Weir = standalone, no series
  - "Project Hail Mary" by Andy Weir = standalone, no series
  - "Warbreaker" by Brandon Sanderson = standalone, no series
- Only set series if you're CERTAIN it's part of a series. When in doubt, leave null.
- CRITICAL: Never replace the actual title with "Book N" - preserve what makes each book unique!

EXAMPLES:
- "Clive Barker - 1986 - The Hellbound Heart (Kafer) 64k" -> Author: Clive Barker, Title: The Hellbound Heart, Narrator: Kafer, series: null
- "Brandon Sanderson - Mistborn #1 - The Final Empire" -> Author: Brandon Sanderson, Title: The Final Empire, series: Mistborn, series_num: 1
- "Christopher Paolini/Eragon" -> Author: Christopher Paolini, Title: Eragon, series: Inheritance Cycle, series_num: 1
- "The Martian" (no author) -> Author: Andy Weir, Title: The Martian, series: null (standalone book)
- "James S.A. Corey - Leviathan Wakes" -> Author: James S.A. Corey, Title: Leviathan Wakes, series: The Expanse, series_num: 1

Return JSON array. Each object MUST have "item" matching the ITEM_N label:
[
  {{"item": "ITEM_1", "author": "Author Name", "title": "Book Title", "narrator": "Narrator or null", "series": "Series Name or null", "series_num": 1, "year": null}}
]

Return ONLY the JSON array, nothing else."""

def parse_json_response(text):
    """Extract JSON from AI response."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return json.loads(text.strip())

def call_ai(messy_names, config):
    """Call AI API to parse book names, with API lookups for context."""
    # First, try to look up each book in metadata APIs
    api_results = []
    for name in messy_names:
        result = lookup_book_metadata(name, config)
        api_results.append(result)
        if result:
            logger.info(f"API lookup success for: {name[:50]}...")

    # Build prompt with API results included
    prompt = build_prompt(messy_names, api_results)
    provider = config.get('ai_provider', 'openrouter')

    # Use selected provider
    if provider == 'ollama':
        # Ollama doesn't need an API key - it's local
        return call_ollama(prompt, config)
    elif provider == 'gemini' and config.get('gemini_api_key'):
        return call_gemini(prompt, config)
    elif config.get('openrouter_api_key'):
        return call_openrouter(prompt, config)
    else:
        logger.error("No API key configured!")
        return None


def explain_http_error(status_code, provider):
    """Convert HTTP status codes to human-readable errors."""
    errors = {
        400: "Bad request - the API didn't understand our request",
        401: "Invalid API key - check your key in Settings",
        403: "Access denied - your API key doesn't have permission",
        404: "Model not found - the selected model may not exist",
        429: "Rate limit exceeded - too many requests, waiting before retry",
        500: f"{provider} server error - their servers are having issues",
        502: f"{provider} is temporarily down - try again later",
        503: f"{provider} is overloaded - try again in a few minutes",
    }
    return errors.get(status_code, f"Unknown error (HTTP {status_code})")


def call_openrouter(prompt, config):
    """Call OpenRouter API."""
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {config['openrouter_api_key']}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/deucebucket/library-manager",
                "X-Title": "Library Metadata Manager"
            },
            json={
                "model": config.get('openrouter_model', 'google/gemma-3n-e4b-it:free'),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1
            },
            timeout=90
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if text:
                return parse_json_response(text)
        else:
            error_msg = explain_http_error(resp.status_code, "OpenRouter")
            logger.warning(f"OpenRouter: {error_msg}")
            # Try to get more detail from response
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"OpenRouter detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("OpenRouter: Request timed out after 90 seconds")
        report_anonymous_error("OpenRouter timeout after 90 seconds", context="openrouter_api")
    except requests.exceptions.ConnectionError:
        logger.error("OpenRouter: Connection failed - check your internet")
        report_anonymous_error("OpenRouter connection failed", context="openrouter_api")
    except Exception as e:
        logger.error(f"OpenRouter: {e}")
        report_anonymous_error(f"OpenRouter error: {e}", context="openrouter_api")
    return None


def call_gemini(prompt, config, retry_count=0):
    """Call Google Gemini API directly with automatic retry on rate limit."""
    try:
        api_key = config.get('gemini_api_key')
        model = config.get('gemini_model', 'gemini-2.0-flash')

        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=90
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                return parse_json_response(text)
        elif resp.status_code == 429 and retry_count < 3:
            # Rate limit - parse retry time and wait
            error_msg = explain_http_error(resp.status_code, "Gemini")
            logger.warning(f"Gemini: {error_msg}")
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"Gemini detail: {detail}")
                    # Try to parse "Please retry in X.XXXs" from message
                    import re
                    match = re.search(r'retry in (\d+\.?\d*)s', detail)
                    if match:
                        wait_time = float(match.group(1)) + 5  # Add 5 sec buffer
                        logger.info(f"Gemini: Waiting {wait_time:.0f} seconds before retry...")
                        time.sleep(wait_time)
                        return call_gemini(prompt, config, retry_count + 1)
            except:
                pass
            # Default wait if we can't parse the time
            wait_time = 45 * (retry_count + 1)
            logger.info(f"Gemini: Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
            return call_gemini(prompt, config, retry_count + 1)
        else:
            error_msg = explain_http_error(resp.status_code, "Gemini")
            logger.warning(f"Gemini: {error_msg}")
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"Gemini detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("Gemini: Request timed out after 90 seconds")
        report_anonymous_error("Gemini timeout after 90 seconds", context="gemini_api")
    except requests.exceptions.ConnectionError:
        logger.error("Gemini: Connection failed - check your internet")
        report_anonymous_error("Gemini connection failed", context="gemini_api")
    except Exception as e:
        logger.error(f"Gemini: {e}")
        report_anonymous_error(f"Gemini error: {e}", context="gemini_api")
    return None


def call_ollama(prompt, config):
    """Call local Ollama API for fully self-hosted AI."""
    try:
        ollama_url = config.get('ollama_url', 'http://localhost:11434')
        model = config.get('ollama_model', 'llama3.2:3b')

        # Ollama's generate endpoint
        resp = requests.post(
            f"{ollama_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1
                }
            },
            timeout=120  # Local models can be slower, especially on first load
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get('response', '')
            if text:
                return parse_json_response(text)
        elif resp.status_code == 404:
            logger.error(f"Ollama: Model '{model}' not found. Run: ollama pull {model}")
        else:
            error_msg = explain_http_error(resp.status_code, "Ollama")
            logger.warning(f"Ollama: {error_msg}")
            try:
                detail = resp.json().get('error', '')
                if detail:
                    logger.warning(f"Ollama detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("Ollama: Request timed out after 120 seconds - model may still be loading")
        report_anonymous_error("Ollama timeout after 120 seconds", context="ollama_api")
    except requests.exceptions.ConnectionError:
        logger.error(f"Ollama: Connection failed - is Ollama running at {config.get('ollama_url', 'http://localhost:11434')}?")
        report_anonymous_error("Ollama connection failed", context="ollama_api")
    except Exception as e:
        logger.error(f"Ollama: {e}")
        report_anonymous_error(f"Ollama error: {e}", context="ollama_api")
    return None


def get_ollama_models(config):
    """Fetch list of available models from Ollama server."""
    try:
        ollama_url = config.get('ollama_url', 'http://localhost:11434')
        resp = requests.get(f"{ollama_url}/api/tags", timeout=10)
        if resp.status_code == 200:
            models = resp.json().get('models', [])
            return [m.get('name', '') for m in models if m.get('name')]
        return []
    except:
        return []


def test_ollama_connection(config):
    """Test connection to Ollama server."""
    try:
        ollama_url = config.get('ollama_url', 'http://localhost:11434')
        resp = requests.get(f"{ollama_url}/api/tags", timeout=10)
        if resp.status_code == 200:
            models = resp.json().get('models', [])
            return {
                'success': True,
                'models': [m.get('name', '') for m in models],
                'model_count': len(models)
            }
        return {'success': False, 'error': f'HTTP {resp.status_code}'}
    except requests.exceptions.ConnectionError:
        return {'success': False, 'error': f'Cannot connect to {ollama_url}'}
    except requests.exceptions.Timeout:
        return {'success': False, 'error': 'Connection timed out'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def extract_audio_sample(audio_file, duration_seconds=90, output_format='mp3'):
    """
    Extract first N seconds of audio file for analysis.
    Returns path to temp file or None on failure.
    """
    import subprocess
    import tempfile

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


def analyze_audio_with_gemini(audio_file, config):
    """
    Send audio sample to Gemini for analysis.
    Extracts author, title, narrator, and series info from audiobook intro.
    Returns dict with extracted info or None on failure.
    """
    import base64

    api_key = config.get('gemini_api_key')
    if not api_key:
        return None

    # Extract audio sample (first 90 seconds)
    sample_path = extract_audio_sample(audio_file, duration_seconds=90)
    if not sample_path:
        logger.debug(f"Could not extract audio sample from {audio_file}")
        return None

    try:
        # Read and encode audio
        with open(sample_path, 'rb') as f:
            audio_data = base64.b64encode(f.read()).decode('utf-8')

        # Clean up temp file
        os.unlink(sample_path)

        # Use gemini-2.5-flash for audio (separate quota from text analysis model)
        model = 'gemini-2.5-flash'

        prompt = """Listen to this audiobook intro and extract the following information.
Many audiobooks start with an announcement like "This is [Title] by [Author], read by [Narrator]".

Extract and return in JSON format:
{
    "title": "book title if mentioned",
    "author": "author name if mentioned",
    "narrator": "narrator name if mentioned",
    "series": "series name if mentioned",
    "language": "ISO 639-1 code of the spoken language (e.g., en, de, fr, es)",
    "confidence": "high/medium/low based on how clearly the info was stated"
}

For the language field:
- Listen to what language the narrator is speaking
- Use the ISO 639-1 two-letter code (en=English, de=German, fr=French, es=Spanish, etc.)
- This should reflect the SPOKEN language, not the original book language

If information is not clearly stated in the audio, use null for that field.
Only include information you actually heard - do not guess."""

        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {
                            "inline_data": {
                                "mime_type": "audio/mp3",
                                "data": audio_data
                            }
                        }
                    ]
                }],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=120  # Audio processing can take longer
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                parsed = parse_json_response(text)
                if parsed:
                    logger.info(f"Audio analysis extracted: {parsed}")
                    return parsed
        else:
            logger.debug(f"Gemini audio API error {resp.status_code}: {resp.text[:200]}")

    except Exception as e:
        logger.debug(f"Audio analysis error: {e}")
        # Clean up temp file if it exists
        if sample_path and os.path.exists(sample_path):
            os.unlink(sample_path)

    return None


def detect_audio_language(audio_file, config):
    """
    Detect the spoken language from an audio file using Gemini.
    This is a lightweight version of analyze_audio_with_gemini focused only on language.

    Args:
        audio_file: Path to audio file
        config: App config with Gemini API key

    Returns:
        dict with 'language' (ISO 639-1 code), 'language_name', and 'confidence', or None
    """
    import base64

    api_key = config.get('gemini_api_key')
    if not api_key:
        logger.debug("No Gemini API key for audio language detection")
        return None

    # Extract shorter audio sample (30 seconds is enough for language detection)
    sample_path = extract_audio_sample(audio_file, duration_seconds=30)
    if not sample_path:
        logger.debug(f"Could not extract audio sample from {audio_file}")
        return None

    try:
        with open(sample_path, 'rb') as f:
            audio_data = base64.b64encode(f.read()).decode('utf-8')

        os.unlink(sample_path)

        model = 'gemini-2.5-flash'

        prompt = """Listen to this audiobook sample and identify the spoken language.

Return JSON only:
{
    "language": "ISO 639-1 two-letter code (en, de, fr, es, it, pt, nl, sv, no, da, fi, pl, ru, ja, zh, ko, etc.)",
    "language_name": "Full language name (English, German, French, etc.)",
    "confidence": "high/medium/low"
}

Focus on the SPOKEN language you hear in the narration, not any background music or sound effects."""

        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "audio/mp3", "data": audio_data}}
                    ]
                }],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=60
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                parsed = parse_json_response(text)
                if parsed and parsed.get('language'):
                    logger.info(f"Audio language detected: {parsed.get('language_name', parsed['language'])} ({parsed.get('confidence', 'unknown')} confidence)")
                    return parsed
        else:
            logger.debug(f"Gemini audio language API error {resp.status_code}")

    except Exception as e:
        logger.debug(f"Audio language detection error: {e}")
        if sample_path and os.path.exists(sample_path):
            os.unlink(sample_path)

    return None


def get_audio_metadata_hints(book_path, config=None):
    """
    Get metadata hints from audio files in a book folder.
    Combines ID3 tags + audio analysis for verification.
    Returns dict with all found hints.
    """
    hints = {}
    audio_files = find_audio_files(str(book_path))

    if not audio_files:
        return hints

    # Sort to get first file (usually has intro)
    audio_files.sort()
    first_file = audio_files[0]

    # Try ID3/metadata extraction first (fast)
    try:
        from mutagen import File as MutagenFile
        audio = MutagenFile(first_file, easy=True)
        if audio:
            if audio.get('artist'):
                hints['id3_author'] = audio.get('artist')[0]
            if audio.get('album'):
                hints['id3_album'] = audio.get('album')[0]
            if audio.get('title'):
                hints['id3_title'] = audio.get('title')[0]
    except ImportError:
        logger.debug("mutagen not installed - skipping ID3 extraction")
    except Exception as e:
        logger.debug(f"ID3 extraction failed: {e}")

    # Audio analysis with Gemini (if enabled and configured)
    if config and config.get('enable_audio_analysis', False) and config.get('gemini_api_key'):
        audio_info = analyze_audio_with_gemini(first_file, config)
        if audio_info:
            if audio_info.get('title'):
                hints['audio_title'] = audio_info['title']
            if audio_info.get('author'):
                hints['audio_author'] = audio_info['author']
            if audio_info.get('narrator'):
                hints['audio_narrator'] = audio_info['narrator']
            if audio_info.get('series'):
                hints['audio_series'] = audio_info['series']
            hints['audio_confidence'] = audio_info.get('confidence', 'unknown')

    return hints


# ============== DEEP SCANNER ==============

import re
import hashlib

# Audio file extensions we care about
AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus', '.wma', '.aac'}
EBOOK_EXTENSIONS = {'.epub', '.pdf', '.mobi', '.azw3'}

# Patterns for disc/chapter folders (these are NOT book titles)
DISC_CHAPTER_PATTERNS = [
    r'^(disc|disk|cd|part|chapter|ch)\s*\d+',  # "Disc 1", "Part 2", "Chapter 3"
    r'^\d+\s*[-_]\s*(disc|disk|cd|part|chapter)',  # "1 - Disc", "01_Chapter"
    r'^(side)\s*[ab12]',  # "Side A", "Side 1"
    r'.+\s*-\s*(disc|disk|cd)\s*\d+$',  # "Book Name - Disc 01"
]

# Junk patterns to clean from titles
JUNK_PATTERNS = [
    r'\[bitsearch\.to\]',
    r'\[rarbg\]',
    r'\(unabridged\)',
    r'\(abridged\)',
    r'\(audiobook\)',
    r'\(audio\)',
    r'\(graphicaudio\)',
    r'\(uk version\)',
    r'\(us version\)',
    r'\[EN\]',
    r'\(r\d+\.\d+\)',  # (r1.0), (r1.1)
    r'\[\d+\]',  # [64420]
    r'\{\d+mb\}',  # {388mb}
    r'\{\d+\.\d+gb\}',  # {1.29gb}
    r'\d+k\s+\d+\.\d+\.\d+',  # 64k 13.31.36
    r'128k|64k|192k|320k',  # bitrate
    r'\.epub$|\.pdf$|\.mobi$',  # file extensions in folder names
]

# Patterns that indicate author name in title
AUTHOR_IN_TITLE_PATTERNS = [
    r'\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*$',  # "Title by Author Name"
    r'^([A-Z][a-z]+,\s+[A-Z][a-z]+)\s*-\s*',  # "LastName, FirstName - Title"
    r'\s+-\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s*$',  # "Title - Author Name"
]


# ============== ORPHAN FILE HANDLING ==============

def read_audio_metadata(file_path):
    """Read ID3/metadata tags from an audio file to identify the book."""
    try:
        from mutagen import File
        from mutagen.easyid3 import EasyID3
        from mutagen.mp3 import MP3
        from mutagen.mp4 import MP4

        audio = File(file_path, easy=True)
        if audio is None:
            return None

        metadata = {}

        # Try to get album (usually the book title for audiobooks)
        if 'album' in audio:
            metadata['album'] = audio['album'][0] if isinstance(audio['album'], list) else audio['album']

        # Try to get artist (sometimes narrator, sometimes author)
        if 'artist' in audio:
            metadata['artist'] = audio['artist'][0] if isinstance(audio['artist'], list) else audio['artist']

        # Try to get album artist (often the author)
        if 'albumartist' in audio:
            metadata['albumartist'] = audio['albumartist'][0] if isinstance(audio['albumartist'], list) else audio['albumartist']

        # Try to get title (track title)
        if 'title' in audio:
            metadata['title'] = audio['title'][0] if isinstance(audio['title'], list) else audio['title']

        return metadata if metadata else None
    except Exception as e:
        logger.debug(f"Could not read metadata from {file_path}: {e}")
        return None


def find_orphan_audio_files(lib_path):
    """Find audio files sitting directly in author folders (not in book subfolders)."""
    orphans = []

    for author_dir in Path(lib_path).iterdir():
        if not author_dir.is_dir():
            continue

        author = author_dir.name

        # Find audio files directly in author folder
        direct_audio = [f for f in author_dir.iterdir()
                       if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]

        if direct_audio:
            # Group files by potential book (using metadata or filename patterns)
            books = {}

            for audio_file in direct_audio:
                # Try to read metadata
                metadata = read_audio_metadata(str(audio_file))

                if metadata and metadata.get('album'):
                    book_title = metadata['album']
                else:
                    # Fallback: try to extract from filename
                    # Pattern: "Book Title - Chapter 01.mp3" or "01 - Chapter Name.mp3"
                    fname = audio_file.stem
                    # Remove chapter/track numbers
                    book_title = re.sub(r'^\d+[\s\-\.]+', '', fname)
                    book_title = re.sub(r'[\s\-]+\d+$', '', book_title)
                    book_title = re.sub(r'\s*-\s*(chapter|part|track|disc)\s*\d*.*$', '', book_title, flags=re.IGNORECASE)

                    if not book_title or book_title == fname:
                        book_title = "Unknown Album"

                if book_title not in books:
                    books[book_title] = []
                books[book_title].append(audio_file)

            for book_title, files in books.items():
                orphans.append({
                    'author': author,
                    'author_path': str(author_dir),
                    'detected_title': book_title,
                    'files': [str(f) for f in files],
                    'file_count': len(files)
                })

    return orphans


def organize_orphan_files(author_path, book_title, files, config=None):
    """Create a book folder and move orphan files into it, including companion files."""
    import shutil

    author_dir = Path(author_path)

    # Clean up the book title for folder name
    clean_title = book_title

    # Remove format/quality junk from title
    clean_title = re.sub(r'\s*\((?:Unabridged|Abridged|MP3|M4B|64k|128k|HQ|Complete|Full|Retail)\)', '', clean_title, flags=re.IGNORECASE)
    clean_title = re.sub(r'\s*\[.*?\]', '', clean_title)  # Remove bracketed content
    clean_title = re.sub(r'[<>:"/\\|?*]', '', clean_title)  # Remove illegal chars
    clean_title = clean_title.strip()

    if not clean_title:
        return False, "Could not determine book title"

    book_dir = author_dir / clean_title

    # Check if folder already exists
    if book_dir.exists():
        # Check if it's empty or has files
        existing = list(book_dir.iterdir())
        if existing:
            return False, f"Folder already exists with {len(existing)} items: {book_dir}"
    else:
        book_dir.mkdir(parents=True)

    # Companion file extensions to also move (covers, metadata, etc.)
    COMPANION_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp',  # covers
                           '.nfo', '.txt', '.json', '.xml', '.cue',   # metadata
                           '.pdf', '.epub', '.mobi',                   # companion ebooks
                           '.srt', '.vtt', '.lrc'}                     # subtitles/lyrics

    # Find companion files in the same directory as the audio files
    companion_files = set()
    source_dirs = set()
    for file_path in files:
        src = Path(file_path)
        if src.exists():
            source_dirs.add(src.parent)

    for source_dir in source_dirs:
        for f in source_dir.iterdir():
            if f.is_file() and f.suffix.lower() in COMPANION_EXTENSIONS:
                companion_files.add(str(f))

    # Combine audio files and companion files
    all_files = list(files) + list(companion_files)

    # Move files
    moved = 0
    errors = []
    for file_path in all_files:
        try:
            src = Path(file_path)
            if src.exists():
                dest = book_dir / src.name
                shutil.move(str(src), str(dest))
                moved += 1
        except Exception as e:
            errors.append(f"{file_path}: {e}")

    # Clean up empty source directories
    for source_dir in source_dirs:
        try:
            if source_dir.exists() and source_dir != author_dir:
                remaining = list(source_dir.iterdir())
                if not remaining:
                    source_dir.rmdir()
                    logger.info(f"Cleaned up empty folder: {source_dir}")
        except OSError:
            pass

    if errors:
        return False, f"Moved {moved} files, {len(errors)} errors: {errors[0]}"

    logger.info(f"Organized {moved} files (audio + companions) into: {book_dir}")
    return True, f"Created {book_dir.name} with {moved} files"


# ============== CHAOS HANDLER - For completely unorganized libraries ==============

def read_audio_metadata_deep(file_path):
    """
    Read all available metadata from an audio file.
    Returns dict with: album, artist, title, track, duration, etc.
    """
    try:
        from mutagen import File
        from mutagen.mp4 import MP4
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3

        audio = File(file_path)
        if audio is None:
            return None

        metadata = {
            'album': None,
            'artist': None,
            'title': None,
            'track': None,
            'duration': None,
            'bitrate': None
        }

        # Get duration
        if hasattr(audio, 'info') and hasattr(audio.info, 'length'):
            metadata['duration'] = audio.info.length
        if hasattr(audio, 'info') and hasattr(audio.info, 'bitrate'):
            metadata['bitrate'] = audio.info.bitrate

        # Handle different formats
        if isinstance(audio, MP4):
            # M4A/M4B files
            metadata['album'] = audio.tags.get('\xa9alb', [None])[0] if audio.tags else None
            metadata['artist'] = audio.tags.get('\xa9ART', [None])[0] if audio.tags else None
            metadata['title'] = audio.tags.get('\xa9nam', [None])[0] if audio.tags else None
        elif isinstance(audio, MP3) and audio.tags:
            # MP3 files with ID3 tags - use raw tag names
            tags = audio.tags
            # ID3v2 tag names: TALB=album, TPE1=artist, TIT2=title, TRCK=track
            if tags.get('TALB'):
                metadata['album'] = str(tags['TALB'].text[0])
            if tags.get('TPE1'):
                metadata['artist'] = str(tags['TPE1'].text[0])
            if tags.get('TIT2'):
                metadata['title'] = str(tags['TIT2'].text[0])
            if tags.get('TRCK'):
                metadata['track'] = str(tags['TRCK'].text[0])
        elif hasattr(audio, 'tags') and audio.tags:
            # Other formats - try EasyID3 style
            tags = audio.tags
            if hasattr(tags, 'get'):
                metadata['album'] = str(tags.get('album', [None])[0]) if tags.get('album') else None
                metadata['artist'] = str(tags.get('artist', [None])[0]) if tags.get('artist') else None
                metadata['title'] = str(tags.get('title', [None])[0]) if tags.get('title') else None
                metadata['track'] = str(tags.get('tracknumber', [None])[0]) if tags.get('tracknumber') else None

        return metadata
    except Exception as e:
        logger.debug(f"Could not read metadata from {file_path}: {e}")
        return None


def group_loose_files(files):
    """
    Intelligently group loose files that likely belong to the same book.
    Groups by: metadata album > filename pattern > file characteristics

    Returns: list of groups, each group is dict with:
        - files: list of file paths
        - group_type: how they were grouped (metadata, pattern, proximity)
        - detected_info: any detected book info
    """
    groups = []
    ungrouped = list(files)

    # === PHASE 1: Group by ID3 album tag ===
    album_groups = {}
    no_album = []

    for f in ungrouped:
        meta = read_audio_metadata_deep(str(f))
        if meta and meta.get('album') and meta['album'].lower() not in ['unknown', 'audiobook', 'untitled']:
            album = meta['album']
            if album not in album_groups:
                album_groups[album] = {
                    'files': [],
                    'artist': meta.get('artist'),
                    'total_duration': 0
                }
            album_groups[album]['files'].append(f)
            if meta.get('duration'):
                album_groups[album]['total_duration'] += meta['duration']
        else:
            no_album.append(f)

    # Add album groups
    for album, data in album_groups.items():
        groups.append({
            'files': data['files'],
            'group_type': 'metadata',
            'detected_info': {
                'title': album,
                'author': data.get('artist'),
                'duration_hours': round(data['total_duration'] / 3600, 1) if data['total_duration'] else None
            }
        })

    ungrouped = no_album

    # === PHASE 2: Group by filename pattern ===
    # Look for: chapter01, part1, disc1, track01, 01, etc.
    pattern_groups = {}
    still_ungrouped = []

    for f in ungrouped:
        fname = f.stem.lower()

        # Extract base name (remove numbers and common suffixes)
        base = re.sub(r'[\s_-]*(chapter|part|track|disc|cd|side)[\s_-]*\d+.*$', '', fname, flags=re.IGNORECASE)
        base = re.sub(r'[\s_-]*\d+[\s_-]*$', '', base)  # Remove trailing numbers
        base = re.sub(r'^\d+[\s_-]*', '', base)  # Remove leading numbers
        base = base.strip(' _-')

        if base and len(base) > 2:
            if base not in pattern_groups:
                pattern_groups[base] = []
            pattern_groups[base].append(f)
        else:
            still_ungrouped.append(f)

    # Add pattern groups (only if more than 1 file - indicates a set)
    for base, file_list in pattern_groups.items():
        if len(file_list) > 1:
            # Calculate total duration
            total_dur = 0
            for f in file_list:
                meta = read_audio_metadata_deep(str(f))
                if meta and meta.get('duration'):
                    total_dur += meta['duration']

            groups.append({
                'files': file_list,
                'group_type': 'pattern',
                'detected_info': {
                    'title': base.replace('_', ' ').replace('-', ' ').title() if base else None,
                    'author': None,
                    'duration_hours': round(total_dur / 3600, 1) if total_dur else None
                }
            })
        else:
            still_ungrouped.extend(file_list)

    ungrouped = still_ungrouped

    # === PHASE 3: Group numbered sequences ===
    # Files like 01.mp3, 02.mp3, 03.mp3 that share same characteristics
    numbered_files = []
    truly_ungrouped = []

    for f in ungrouped:
        fname = f.stem
        # Check if filename is just a number or very short
        if re.match(r'^\d{1,3}$', fname) or len(fname) <= 3:
            numbered_files.append(f)
        else:
            truly_ungrouped.append(f)

    if numbered_files:
        # Sort by name to keep sequence
        numbered_files.sort(key=lambda x: x.stem)
        total_dur = sum(
            (read_audio_metadata_deep(str(f)) or {}).get('duration', 0)
            for f in numbered_files
        )
        groups.append({
            'files': numbered_files,
            'group_type': 'sequence',
            'detected_info': {
                'title': None,  # Unknown - needs identification
                'author': None,
                'duration_hours': round(total_dur / 3600, 1) if total_dur else None,
                'needs_identification': True
            }
        })

    # === PHASE 4: Individual files that couldn't be grouped ===
    for f in truly_ungrouped:
        meta = read_audio_metadata_deep(str(f))
        groups.append({
            'files': [f],
            'group_type': 'single',
            'detected_info': {
                'title': meta.get('title') or f.stem if meta else f.stem,
                'author': meta.get('artist') if meta else None,
                'duration_hours': round(meta['duration'] / 3600, 1) if meta and meta.get('duration') else None
            }
        })

    return groups


def search_bookdb_api(title):
    """
    Search the BookBucket API for a book (public endpoint, no auth needed).
    Uses Qdrant vector search - fast even with 50M books.
    Returns dict with author, title, series if found.
    Filters garbage matches using title similarity.
    """
    # Clean the search title (remove "audiobook", file extensions, etc.)
    search_title = clean_search_title(title)
    if not search_title or len(search_title) < 3:
        return None

    # Skip unsearchable queries (chapter1, track05, etc.)
    if is_unsearchable_query(search_title):
        logger.debug(f"BookDB API: Skipping unsearchable query '{search_title}'")
        return None

    try:
        # Use longer timeout for cold start (embedding model can take 45-60s to load)
        # Retry once on timeout
        for attempt in range(2):
            try:
                response = requests.get(
                    f"{BOOKDB_API_URL}/search",
                    params={"q": search_title, "limit": 5},
                    timeout=60 if attempt == 0 else 30
                )
                break
            except requests.exceptions.Timeout:
                if attempt == 0:
                    logger.debug(f"BookDB API timeout on first attempt, retrying...")
                    continue
                raise

        if response.status_code == 200:
            results = response.json()

            # Find best book match (prefer books over series)
            for item in results:
                if item.get('type') == 'book' and item.get('author_name'):
                    suggested_title = item.get('name', '')

                    # Filter garbage matches - reject low similarity
                    if is_garbage_match(search_title, suggested_title):
                        logger.debug(f"BookDB API: Rejected garbage match '{search_title}' -> '{suggested_title}'")
                        continue

                    result_author = item.get('author_name', '')

                    # TRUST EXISTING AUTHORS: If we have a valid (non-placeholder) author,
                    # keep it even if API returns a different author. Same title can exist
                    # by different authors - "The Destroyer of Worlds" by Matt Ruff is different
                    # from "The Destroyer of Worlds" by someone else.
                    if author and result_author and not is_placeholder_author(author):
                        author_is_drastic = is_drastic_author_change(author, result_author)
                        if author_is_drastic:
                            # Found title match, but keep ORIGINAL author (trust folder structure)
                            logger.debug(f"BookDB API: Title match found but keeping original author '{author}' (API had '{result_author}')")
                            # Don't change author - use original
                        else:
                            # Authors are similar (same person, different format) - use API version
                            author = result_author
                    else:
                        # No original author or it's a placeholder - use API author
                        author = result_author
                    # Fix author format (some have "Last, First")
                    if ',' in author and author.count(',') == 1:
                        parts = author.split(',')
                        author = f"{parts[1].strip()} {parts[0].strip()}"

                    return {
                        'title': suggested_title,
                        'author': author,
                        'series': item.get('series_name'),
                        'year': item.get('year_published'),
                        'source': 'bookdb_api'
                    }

            # Fallback to series if no book match
            for item in results:
                if item.get('type') == 'series' and item.get('author_name'):
                    suggested_title = item.get('name', '')

                    # Filter garbage matches
                    if is_garbage_match(search_title, suggested_title):
                        continue

                    result_author = item.get('author_name', '')

                    # TRUST EXISTING AUTHORS for series too
                    if author and result_author and not is_placeholder_author(author):
                        author_is_drastic = is_drastic_author_change(author, result_author)
                        if author_is_drastic:
                            logger.debug(f"BookDB API: Series match but keeping original author '{author}' (API had '{result_author}')")
                            # Don't change author
                        else:
                            author = result_author
                    else:
                        author = result_author
                    if ',' in author and author.count(',') == 1:
                        parts = author.split(',')
                        author = f"{parts[1].strip()} {parts[0].strip()}"

                    return {
                        'title': suggested_title,
                        'author': author,
                        'series': item.get('name'),
                        'source': 'bookdb_api'
                    }

    except Exception as e:
        logger.debug(f"BookDB API search error: {e}")

    return None


def search_book_searxng(query, duration_hours=None):
    """
    Search for a book using SearXNG.
    Optionally filter by expected audiobook length.
    """
    try:
        # Add audiobook context to query
        search_query = f"{query} audiobook"
        if duration_hours and duration_hours > 1:
            search_query += f" {int(duration_hours)} hours"

        # Use local SearXNG instance
        response = requests.get(
            "http://localhost:8888/search",
            params={
                'q': search_query,
                'format': 'json',
                'categories': 'general'
            },
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])[:5]

            # Extract book info from results
            books = []
            for r in results:
                title = r.get('title', '')
                content = r.get('content', '')

                # Look for author patterns
                author_match = re.search(r'by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', title + ' ' + content)

                books.append({
                    'title': title,
                    'snippet': content[:200],
                    'url': r.get('url'),
                    'author': author_match.group(1) if author_match else None
                })

            return books
    except Exception as e:
        logger.debug(f"SearXNG search failed: {e}")

    return []


def identify_book_with_ai(file_group, config):
    """
    Use AI to identify a book from file information.
    Sends filenames, duration, and any metadata to AI for identification.
    """
    if not config:
        return None

    files = file_group.get('files', [])
    info = file_group.get('detected_info', {})

    # Build context for AI
    filenames = [Path(f).name if isinstance(f, str) else f.name for f in files[:20]]

    prompt = f"""Identify this audiobook from the following information:

Files ({len(files)} total): {', '.join(filenames[:10])}{'...' if len(filenames) > 10 else ''}
Total duration: {info.get('duration_hours', 'unknown')} hours
Detected album tag: {info.get('title', 'none')}
Detected artist tag: {info.get('author', 'none')}

Based on this information, identify the audiobook. Return JSON:
{{"author": "Author Name", "title": "Book Title", "series": "Series Name or null", "confidence": "high/medium/low"}}

If you cannot identify it with reasonable confidence, return:
{{"author": null, "title": null, "confidence": "none", "reason": "why"}}"""

    try:
        # Use existing AI call function
        gemini_key = None
        secrets = load_secrets()
        if secrets:
            gemini_key = secrets.get('gemini_api_key')

        if gemini_key:
            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{config.get('gemini_model', 'gemini-2.0-flash')}:generateContent",
                headers={'Content-Type': 'application/json'},
                params={'key': gemini_key},
                json={'contents': [{'parts': [{'text': prompt}]}]},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')

                # Parse JSON from response
                json_match = re.search(r'\{[^}]+\}', text, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group())
    except Exception as e:
        logger.debug(f"AI identification failed: {e}")

    return None


def transcribe_audio_clip(file_path, duration_seconds=30):
    """
    Transcribe a short clip from an audio file for identification.
    Uses Whisper API or local Whisper if available.

    Returns: transcribed text or None
    """
    try:
        # Try using OpenAI Whisper API first (if we have a key)
        secrets = load_secrets()
        openai_key = secrets.get('openai_api_key') if secrets else None

        if openai_key:
            # Use OpenAI Whisper API
            import subprocess
            import tempfile

            # Extract a clip using ffmpeg
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp_path = tmp.name

            # Extract 30 seconds starting at 60 seconds in (skip intro)
            subprocess.run([
                'ffmpeg', '-y', '-i', str(file_path),
                '-ss', '60', '-t', str(duration_seconds),
                '-acodec', 'libmp3lame', '-ar', '16000',
                tmp_path
            ], capture_output=True, timeout=30)

            # Send to Whisper API
            with open(tmp_path, 'rb') as audio_file:
                response = requests.post(
                    'https://api.openai.com/v1/audio/transcriptions',
                    headers={'Authorization': f'Bearer {openai_key}'},
                    files={'file': audio_file},
                    data={'model': 'whisper-1'},
                    timeout=60
                )

            os.unlink(tmp_path)

            if response.status_code == 200:
                return response.json().get('text')

    except Exception as e:
        logger.debug(f"Audio transcription failed: {e}")

    return None


def search_by_transcription(transcription, config):
    """
    Search for a book using transcribed audio text.
    Searches our BookDB and uses AI to match.
    """
    if not transcription or len(transcription) < 50:
        return None

    # Take a meaningful chunk
    chunk = transcription[:500]

    # Search SearXNG for the quote
    try:
        response = requests.get(
            "http://localhost:8888/search",
            params={
                'q': f'"{chunk[:100]}" audiobook',
                'format': 'json'
            },
            timeout=10
        )

        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                # Use AI to analyze results
                return identify_from_search_results(results, chunk, config)
    except Exception as e:
        logger.debug(f"Transcription search failed: {e}")

    return None


def identify_from_search_results(results, context, config):
    """Use AI to identify book from search results and context."""
    if not config:
        return None

    secrets = load_secrets()
    gemini_key = secrets.get('gemini_api_key') if secrets else None

    if not gemini_key:
        return None

    results_text = "\n".join([
        f"- {r.get('title', 'No title')}: {r.get('content', '')[:100]}"
        for r in results[:5]
    ])

    prompt = f"""Based on this transcribed audio excerpt and search results, identify the audiobook:

Transcribed text: "{context[:300]}"

Search results:
{results_text}

Return JSON: {{"author": "Name", "title": "Title", "confidence": "high/medium/low"}}
If unsure, return {{"confidence": "none"}}"""

    try:
        response = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{config.get('gemini_model', 'gemini-2.0-flash')}:generateContent",
            headers={'Content-Type': 'application/json'},
            params={'key': gemini_key},
            json={'contents': [{'parts': [{'text': prompt}]}]},
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            json_match = re.search(r'\{[^}]+\}', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
    except Exception as e:
        logger.debug(f"AI identification from results failed: {e}")

    return None


def handle_chaos_library(lib_path, config=None):
    """
    Handle a completely chaotic library - files dumped directly in root with no structure.

    Process:
    1. Find all loose audio files
    2. Group related files (by metadata, patterns, etc.)
    3. Identify each group using multiple methods
    4. Create proper Author/Title structure

    Returns: list of results with actions taken
    """
    lib_root = Path(lib_path)
    results = []

    # Find all loose audio files in root
    loose_files = [
        f for f in lib_root.iterdir()
        if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS
    ]

    if not loose_files:
        return [{'status': 'ok', 'message': 'No loose files found in library root'}]

    logger.info(f"CHAOS HANDLER: Found {len(loose_files)} loose files in library root")

    # Group the files
    groups = group_loose_files(loose_files)
    logger.info(f"CHAOS HANDLER: Grouped into {len(groups)} potential books")

    # Initialize progress tracker with queue of items
    queue_items = [g.get('detected_info', {}).get('title', f'Group {i+1}') for i, g in enumerate(groups)]
    search_progress.start('chaos_scan', len(groups), queue_items)

    for i, group in enumerate(groups):
        files = group['files']
        info = group['detected_info']
        group_type = group['group_type']

        logger.info(f"CHAOS HANDLER: Processing group {i+1}/{len(groups)} ({len(files)} files, type={group_type})")

        result = {
            'files': [str(f) for f in files],
            'file_count': len(files),
            'group_type': group_type,
            'detected_info': info
        }

        author = info.get('author')
        title = info.get('title')
        confidence = 'high' if group_type == 'metadata' else 'low'

        # === IDENTIFICATION PIPELINE ===

        # Level 1: Already have metadata
        if author and title and group_type == 'metadata':
            result['identification'] = 'metadata'
            result['author'] = author
            result['title'] = title
            result['confidence'] = 'high'

        # Level 2: Search by detected title/filename
        elif title:
            # Try BookBucket API first (50M books, public endpoint, fast)
            search_progress.set_status(f"Searching BookDB for '{title[:30]}...'")
            api_result = search_bookdb_api(title)
            if api_result and api_result.get('author'):
                author = api_result.get('author')
                title = api_result.get('title') or title
                confidence = 'high'
                result['identification'] = 'bookdb_api'
                search_progress.set_status(f"Found in BookDB: {author}")
                if api_result.get('series'):
                    result['series'] = api_result.get('series')

            # Fall back to AI if API didn't find it
            if not author:
                search_progress.set_status(f"BookDB no match, trying AI for '{title[:30]}...'")
                ai_result = identify_book_with_ai(group, config)
                if ai_result and ai_result.get('author'):
                    author = ai_result.get('author')
                    title = ai_result.get('title') or title
                    confidence = ai_result.get('confidence', 'medium')
                    result['identification'] = 'ai'
                    search_progress.set_status(f"AI identified: {author}")
                    if ai_result.get('series'):
                        result['series'] = ai_result.get('series')
                else:
                    search_progress.set_status(f"Could not identify '{title[:30]}...'")

            # Track if we had to fall back
            if result.get('identification') == 'ai' and api_result is None:
                result['fallback_reason'] = 'BookDB unavailable or no match'

            result['author'] = author or 'Unknown Author'
            result['title'] = title
            result['confidence'] = confidence

        # Level 3: Numbered/unknown files - need transcription
        elif info.get('needs_identification') and len(files) > 0:
            logger.info(f"CHAOS HANDLER: Attempting audio transcription for unknown group")
            search_progress.set_status("No title detected, trying audio transcription...")

            # Try transcription on first file
            transcription = transcribe_audio_clip(str(files[0]))
            if transcription:
                search_progress.set_status("Transcription complete, searching...")
                trans_result = search_by_transcription(transcription, config)
                if trans_result and trans_result.get('confidence') != 'none':
                    author = trans_result.get('author')
                    title = trans_result.get('title')
                    confidence = trans_result.get('confidence', 'low')
                    result['identification'] = 'transcription'
                    result['transcription_sample'] = transcription[:200]
                    search_progress.set_status(f"Transcription identified: {author}")
                else:
                    search_progress.set_status("Transcription search found no match")
            else:
                search_progress.set_status("Audio transcription failed")

            result['author'] = author or 'Unknown Author'
            result['title'] = title or f"Unknown Book ({len(files)} files, {info.get('duration_hours', '?')}h)"
            result['confidence'] = confidence

        else:
            result['author'] = 'Unknown Author'
            result['title'] = title or f"Unknown ({len(files)} files)"
            result['confidence'] = 'none'
            result['identification'] = 'failed'
            search_progress.set_status("Could not identify - no title or metadata")

        # Update progress
        item_name = result.get('title') or f'Group {i+1}'
        search_progress.update(item_name, result)

        results.append(result)

    # Mark progress complete
    search_progress.finish()

    return results


def is_disc_chapter_folder(name):
    """Check if folder name looks like a disc/chapter subfolder."""
    name_lower = name.lower()
    for pattern in DISC_CHAPTER_PATTERNS:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return True
    return False


def clean_title(title):
    """Remove junk from title, return (cleaned_title, issues_found)."""
    issues = []
    cleaned = title

    for pattern in JUNK_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            issues.append(f"junk: {pattern}")
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    # Clean up extra whitespace and dashes
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^[-_\s]+|[-_\s]+$', '', cleaned)
    cleaned = re.sub(r'\s*-\s*$', '', cleaned)

    return cleaned, issues


def analyze_full_path(audio_file_path, library_root):
    """
    Analyze the COMPLETE path from library root to audio file.
    Works BACKWARDS from the file to understand the structure.

    Returns dict with:
        - book_folder: Path to the folder containing this book's audio
        - detected_author: Best guess at author name
        - detected_title: Best guess at book title
        - detected_series: Series name if detected
        - folder_roles: Dict mapping each folder to its detected role
        - confidence: How confident we are in the detection
        - issues: List of potential problems
    """
    audio_path = Path(audio_file_path)
    lib_root = Path(library_root)

    # Get relative path from library root
    try:
        rel_path = audio_path.relative_to(lib_root)
    except ValueError:
        return None  # File not under library root

    parts = list(rel_path.parts)
    if len(parts) < 2:  # Just filename, no folder structure
        return {
            'book_folder': str(lib_root),
            'detected_author': 'Unknown',
            'detected_title': audio_path.stem,
            'detected_series': None,
            'folder_roles': {},
            'confidence': 'low',
            'issues': ['loose_file_no_structure']
        }

    # Remove filename, work with folders only
    filename = parts[-1]
    folders = parts[:-1]

    # Classify each folder from BOTTOM to TOP
    folder_roles = {}
    issues = []

    def looks_like_person_name(name):
        """Check if name looks like a person's name (First Last pattern)."""
        patterns = [
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',           # First Last
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+$',  # First Middle Last
            r'^[A-Z]\.\s*[A-Z][a-z]+$',               # F. Last
            r'^[A-Z][a-z]+\s+[A-Z]\.\s*[A-Z][a-z]+$', # First M. Last
            r'^[A-Z][a-z]+,\s+[A-Z][a-z]+$',          # Last, First
            r'^[A-Z]\.([A-Z]\.)+\s*[A-Z][a-z]+$',     # J.R.R. Tolkien
        ]
        return any(re.match(p, name) for p in patterns)

    def looks_like_disc_chapter(name):
        """Check if folder is a disc/chapter/part folder (not meaningful for title)."""
        patterns = [
            r'^(disc|disk|cd|dvd)\s*\d+',
            r'^(part|chapter|ch)\s*\d+',
            r'^\d+\s*[-–]\s*(disc|disk|cd|part)',
            r'^(side)\s*[ab12]',
            r'^\d{1,2}$',  # Just a number like "1", "01"
        ]
        return any(re.search(p, name, re.IGNORECASE) for p in patterns)

    def looks_like_book_number(name):
        """Check if folder indicates a numbered book in series."""
        patterns = [
            r'^(book|vol|volume|part)\s*\d+',
            r'^\d+\s*[-–:.]\s*\w',  # "01 - Title", "1. Title"
            r'^#?\d+\s*[-–:]',      # "#1 - Title"
        ]
        return any(re.search(p, name, re.IGNORECASE) for p in patterns)

    def looks_like_title_with_year(name):
        """Check if name looks like a title with a year (series/book name)."""
        return bool(re.search(r'\b(19[0-9]{2}|20[0-9]{2})\b', name))

    def looks_like_series_name(name):
        """Check if name looks like a series name."""
        # Series often have: numbers, "series", "saga", "chronicles", or are the same as child folder
        patterns = [
            r'\bseries\b', r'\bsaga\b', r'\bchronicles\b', r'\btrilogy\b',
            r'\bcycle\b', r'\buniverse\b', r'\bbooks?\b',
        ]
        return any(re.search(p, name, re.IGNORECASE) for p in patterns)

    def is_known_series(name):
        """
        Check if name matches a series in our database (with fuzzy matching).
        Returns: (found: bool, lookup_succeeded: bool)
        - (True, True) = found in database
        - (False, True) = not found, but lookup worked
        - (False, False) = lookup failed (connection error, etc)
        """
        try:
            conn = get_bookdb_connection()
            if conn:
                cursor = conn.cursor()
                # Clean name for search
                clean_name = re.sub(r'[^\w\s]', '', name).strip()
                # Try exact match first
                cursor.execute("SELECT COUNT(*) FROM series WHERE LOWER(name) = LOWER(?)", (clean_name,))
                count = cursor.fetchone()[0]
                if count > 0:
                    conn.close()
                    return (True, True)
                # Try fuzzy match - handle "Dark Tower" matching "The Dark Tower"
                # Also handle "Wheel of Time" matching "The Wheel of Time"
                cursor.execute(
                    "SELECT COUNT(*) FROM series WHERE LOWER(name) LIKE ? OR LOWER(name) LIKE ?",
                    (f'%{clean_name.lower()}%', f'%the {clean_name.lower()}%')
                )
                count = cursor.fetchone()[0]
                conn.close()
                return (count > 0, True)
        except Exception as e:
            logging.debug(f"Series lookup failed for '{name}': {e}")
        return (False, False)  # Lookup failed

    def is_known_author(name):
        """
        Check if name matches an author in our database.
        Returns: (found: bool, lookup_succeeded: bool)
        - (True, True) = found in database
        - (False, True) = not found, but lookup worked
        - (False, False) = lookup failed (connection error, etc)
        """
        try:
            conn = get_bookdb_connection()
            if conn:
                cursor = conn.cursor()
                clean_name = re.sub(r'[^\w\s\.]', '', name).strip()
                cursor.execute("SELECT COUNT(*) FROM authors WHERE LOWER(name) = LOWER(?)", (clean_name,))
                count = cursor.fetchone()[0]
                conn.close()
                return (count > 0, True)
        except Exception as e:
            logging.debug(f"Author lookup failed for '{name}': {e}")
        return (False, False)  # Lookup failed

    # Work from bottom (closest to files) to top
    detected_author = None
    detected_title = None
    detected_series = None
    book_folder_idx = None

    for i in range(len(folders) - 1, -1, -1):
        folder = folders[i]

        if looks_like_disc_chapter(folder):
            folder_roles[folder] = 'disc_chapter'
            continue

        # First non-disc folder from bottom is likely the book title
        if book_folder_idx is None:
            book_folder_idx = i
            folder_roles[folder] = 'book_title'
            detected_title = folder

            # Check if this looks like "SeriesName Book N - ActualTitle"
            book_num_match = re.match(r'^(.+?)\s*(?:book|vol)\s*\d+\s*[-–:]\s*(.+)$', folder, re.IGNORECASE)
            if book_num_match:
                detected_series = book_num_match.group(1).strip()
                detected_title = book_num_match.group(2).strip()
            continue

        # Check what this parent folder looks like
        # Priority: database matches > pattern matches > position-based guesses

        # First check database for definitive matches
        # Returns (found, lookup_succeeded) tuples
        author_result = is_known_author(folder)
        series_result = is_known_series(folder)

        # Extract results - if lookup failed, treat as "unknown" not "not found"
        db_is_author = author_result[0] if author_result[1] else None  # None = lookup failed
        db_is_series = series_result[0] if series_result[1] else None

        # Position-aware disambiguation: if we're between author and book, lean towards series
        # Check if parent folder looks like an author (person name pattern)
        parent_is_person = i > 0 and looks_like_person_name(folders[i-1])
        is_middle_position = book_folder_idx is not None and i < book_folder_idx

        # If lookups failed, fall back to pattern-only detection (no database assumptions)
        if db_is_author is None or db_is_series is None:
            # Database unavailable - use patterns only, don't make assumptions
            if 'db_lookup_failed' not in issues:
                issues.append('db_lookup_failed')
            if looks_like_person_name(folder):
                folder_roles[folder] = 'author'
                detected_author = folder
            elif looks_like_book_number(folder):
                folder_roles[folder] = 'book_number'
            elif looks_like_series_name(folder) or looks_like_title_with_year(folder):
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
            elif detected_author is None:
                if parent_is_person and is_middle_position:
                    folder_roles[folder] = 'series'
                    detected_series = folder
                else:
                    folder_roles[folder] = 'likely_author'
                    detected_author = folder
            continue  # Skip to next folder

        if db_is_author and not db_is_series:
            # Found in authors DB but not series - but check position context
            if parent_is_person and is_middle_position and not looks_like_person_name(folder):
                # Parent looks like author, we're in middle, treat as series
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
            else:
                folder_roles[folder] = 'author'
                detected_author = folder
        elif db_is_series and not db_is_author:
            # Definitely a series from our database
            folder_roles[folder] = 'series'
            if detected_series is None:
                detected_series = folder
        elif db_is_author and db_is_series:
            # Ambiguous - found in both databases
            # Priority: position context > name pattern
            if parent_is_person and is_middle_position:
                # Strong contextual signal: parent looks like author, we're between author and book
                # This is likely a series even if the name looks like a person
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
            elif looks_like_person_name(folder) and not is_middle_position:
                # Looks like a name AND not in a series position - treat as author
                folder_roles[folder] = 'author'
                detected_author = folder
            else:
                # Default to series when ambiguous
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
        elif looks_like_person_name(folder):
            folder_roles[folder] = 'author'
            detected_author = folder
        elif looks_like_book_number(folder):
            # This folder is a book number, so parent of THAT is probably series or author
            folder_roles[folder] = 'book_number'
            # The detected_title should be updated if we have a better one
            if detected_title and looks_like_book_number(detected_title):
                # Our "title" was actually a book number folder
                detected_title = folder
        elif looks_like_series_name(folder) or looks_like_title_with_year(folder):
            folder_roles[folder] = 'series'
            if detected_series is None:
                detected_series = folder
        elif detected_author is None:
            # Contextual guess: if we already have a book title and this folder
            # is between where author should be and the book, it's likely a series
            # Structure: Author / Series / BookTitle
            if book_folder_idx is not None and i < book_folder_idx and not looks_like_person_name(folder):
                # This is a middle folder - check if parent might be author
                if i > 0 and looks_like_person_name(folders[i-1]):
                    folder_roles[folder] = 'series'
                    detected_series = folder
                else:
                    # Assume author at top level
                    folder_roles[folder] = 'likely_author'
                    detected_author = folder
            else:
                folder_roles[folder] = 'likely_author'
                detected_author = folder

    # Build book folder path
    if book_folder_idx is not None:
        book_folder = lib_root / Path(*folders[:book_folder_idx + 1])
    else:
        book_folder = lib_root / Path(*folders)

    # Validate and add issues
    if detected_author and not looks_like_person_name(detected_author):
        issues.append(f'author_not_name_pattern:{detected_author}')

    if detected_author and looks_like_title_with_year(detected_author):
        issues.append(f'author_looks_like_title:{detected_author}')

    if detected_title and looks_like_person_name(detected_title):
        issues.append(f'title_looks_like_author:{detected_title}')

    # Check for likely reversed structure
    if (detected_author and detected_title and
        looks_like_title_with_year(detected_author) and
        looks_like_person_name(detected_title)):
        issues.append('STRUCTURE_LIKELY_REVERSED')
        # Swap them
        detected_author, detected_title = detected_title, detected_author

    # Confidence level
    if detected_author and looks_like_person_name(detected_author):
        confidence = 'high'
    elif detected_author:
        confidence = 'medium'
    else:
        confidence = 'low'

    return {
        'book_folder': str(book_folder),
        'detected_author': detected_author or 'Unknown',
        'detected_title': detected_title or audio_path.stem,
        'detected_series': detected_series,
        'folder_roles': folder_roles,
        'confidence': confidence,
        'issues': issues,
        'depth': len(folders)
    }


def analyze_path_with_ai(full_path, library_root, config, sample_files=None):
    """
    Use Gemini AI to analyze an ambiguous folder path.
    Called when script-based analysis has low confidence.

    Args:
        full_path: Full path to the book folder
        library_root: Root of the library
        config: App config with API keys
        sample_files: Optional list of audio filenames in the folder
    """
    try:
        rel_path = Path(full_path).relative_to(library_root)
        path_str = str(rel_path)
    except ValueError:
        path_str = full_path

    # Build context about the files
    files_context = ""
    if sample_files:
        files_context = f"\nAudio files in this folder: {', '.join(sample_files[:10])}"
        if len(sample_files) > 10:
            files_context += f" (and {len(sample_files) - 10} more)"

    prompt = f"""Analyze this audiobook folder path and identify the structure.

PATH: {path_str}{files_context}

For audiobook libraries, folders typically represent:
- Author name (person's name like "Brandon Sanderson", "J.R.R. Tolkien")
- Series name (like "The Wheel of Time", "Metro 2033", "Mistborn")
- Book title (the actual book name)
- Disc/Part folders (like "Disc 1", "CD1", "Part 1" - ignore these for metadata)

Analyze this path and determine:
1. Which folder is the AUTHOR (should be a person's name)
2. Which folder is the SERIES (if any - optional)
3. Which folder is the BOOK TITLE
4. Is the structure correct (Author/Series/Book or Author/Book) or reversed?

IMPORTANT:
- A year like "2033" or "1984" in a folder name usually means it's a TITLE, not an author
- Two capitalized words that look like "First Last" are likely an AUTHOR
- If author and title seem swapped, indicate the correct order

Return JSON only:
{{
    "detected_author": "Author Name",
    "detected_series": "Series Name or null",
    "detected_title": "Book Title",
    "structure_correct": true/false,
    "suggested_path": "Correct/Path/Structure",
    "confidence": "high/medium/low",
    "reasoning": "Brief explanation"
}}"""

    result = call_gemini(prompt, config)
    if result:
        return result
    return None


def smart_analyze_path(audio_file_or_folder, library_root, config):
    """
    Smart path analysis - tries script first, falls back to AI if needed.

    Returns the analysis result with author, title, series, and any issues.
    """
    path = Path(audio_file_or_folder)

    # If it's a folder, find an audio file inside
    if path.is_dir():
        audio_files = list(path.rglob('*'))
        audio_files = [f for f in audio_files if f.suffix.lower() in AUDIO_EXTENSIONS]
        if audio_files:
            audio_file = str(audio_files[0])
            sample_files = [f.name for f in audio_files[:15]]
        else:
            return {'error': 'No audio files found'}
    else:
        audio_file = str(path)
        sample_files = [path.name]

    # First try script-based analysis
    script_result = analyze_full_path(audio_file, library_root)

    if script_result is None:
        return {'error': 'Path not under library root'}

    # If confidence is high and no major issues, use script result
    if script_result['confidence'] == 'high' and 'STRUCTURE_LIKELY_REVERSED' not in script_result.get('issues', []):
        script_result['method'] = 'script'
        return script_result

    # For low confidence or issues, try AI
    logger.info(f"Script confidence {script_result['confidence']}, trying AI for: {audio_file_or_folder}")

    ai_result = analyze_path_with_ai(
        str(path) if path.is_dir() else str(path.parent),
        library_root,
        config,
        sample_files
    )

    if ai_result:
        ai_result['method'] = 'ai'
        ai_result['script_fallback'] = script_result
        return ai_result

    # Fall back to script result if AI fails
    script_result['method'] = 'script_fallback'
    return script_result


def analyze_author(author):
    """Analyze author name for issues, return list of issues."""
    issues = []

    # System/junk folder names - these should NEVER be processed as books
    system_folders = {'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
                      'backup', 'backups', 'old', 'new', 'test', 'tests', 'sample', 'samples',
                      '.thumbnails', 'thumbnails', 'covers', 'images', 'artwork', 'art',
                      'extras', 'bonus', 'misc', 'other', 'various', 'unknown', 'unsorted',
                      'downloads', 'incoming', 'processing', 'completed', 'done', 'failed',
                      'streams', 'chapters', 'parts', 'disc', 'disk', 'cd', 'dvd'}
    if author.lower() in system_folders:
        issues.append("system_folder_not_author")
        return issues  # Don't bother checking anything else

    # Year in author name
    if re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', author):
        issues.append("year_in_author")

    # Words that are clearly NOT first names (adjectives, articles, title starters)
    not_first_names = {'last', 'first', 'final', 'dark', 'shadow', 'night', 'blood', 'death',
                       'city', 'house', 'world', 'kingdom', 'empire', 'war', 'game', 'fire',
                       'ice', 'storm', 'the', 'a', 'an', 'of', 'and', 'in', 'to', 'for',
                       'new', 'old', 'black', 'white', 'red', 'blue', 'green', 'golden',
                       'lost', 'forgotten', 'hidden', 'secret', 'ancient', 'eternal'}

    # Words that are clearly NOT surnames (plural nouns, abstract concepts)
    not_surnames = {'chances', 'secrets', 'lies', 'dreams', 'tales', 'chronicles', 'stories',
                    'wishes', 'memories', 'shadows', 'nights', 'days', 'years', 'wars',
                    'games', 'fires', 'storms', 'kingdoms', 'empires', 'worlds', 'houses',
                    'cities', 'deaths', 'lives', 'loves', 'hearts', 'souls', 'minds',
                    'stars', 'moons', 'suns', 'gods', 'demons', 'angels', 'dragons',
                    'kings', 'queens', 'lords', 'princes', 'witches', 'wizards'}

    author_words = author.lower().split()

    # Check if it structurally looks like a name
    name_patterns = [
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',           # First Last (exact)
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+$',  # First Middle Last
        r'^[A-Z]\.\s*[A-Z][a-z]+$',               # F. Last
        r'^[A-Z][a-z]+\s+[A-Z]\.\s*[A-Z][a-z]+$', # First M. Last
        r'^[A-Z][a-z]+,\s+[A-Z][a-z]+$',          # Last, First
        r'^[A-Z][a-z]+$',                          # Single name (Plato, Madonna)
        r'^[A-Z]\.([A-Z]\.)+\s*[A-Z][a-z]+$',     # J.R.R. Tolkien, H.P. Lovecraft
        r'^[A-Z][a-z]+\s+[A-Z]\.([A-Z]\.)+\s*[A-Z][a-z]+$',  # George R.R. Martin
        r'^[A-Z][a-z]+\s+[A-Z]\.[A-Z]\.\s*[A-Z][a-z]+$',     # Brandon R.R. Author
        r'^[A-Z][a-z]+\s+[A-Z]\.\s*(Le|De|Von|Van|La|Du)\s+[A-Z][a-z]+$',  # Ursula K. Le Guin
        r'^[A-Z][a-z]+\s+(Le|De|Von|Van|La|Du)\s+[A-Z][a-z]+$',  # Anne De Vries
    ]
    looks_like_name = any(re.match(p, author) for p in name_patterns)

    # Even if it LOOKS like a name structurally, check if the words are actually name-like
    if looks_like_name and len(author_words) >= 2:
        first_word = author_words[0]
        last_word = author_words[-1]

        # "Last Chances" - first word is adjective, last word is plural noun = NOT a name
        if first_word in not_first_names and last_word in not_surnames:
            looks_like_name = False
            issues.append("title_fragment_not_name")
        # "Last Something" - first word alone is a red flag if not a real first name
        elif first_word in not_first_names and last_word in not_surnames:
            looks_like_name = False
            issues.append("title_words_in_author")
        # "Something Chances" - second word is clearly not a surname
        elif last_word in not_surnames:
            looks_like_name = False
            issues.append("not_a_surname")

    # Only flag title words if it DOESN'T look like a valid name
    if not looks_like_name:
        title_words = ['the', 'of', 'and', 'a', 'in', 'to', 'for', 'book', 'series', 'volume',
                       'last', 'first', 'final', 'dark', 'shadow', 'night', 'blood', 'death',
                       'city', 'house', 'world', 'kingdom', 'empire', 'war', 'game', 'fire',
                       'ice', 'storm', 'king', 'queen', 'lord', 'lady', 'prince', 'dragon',
                       'chances', 'secrets', 'lies', 'dreams', 'tales', 'chronicles']
        if any(w in author_words for w in title_words):
            issues.append("title_words_in_author")

        # Two+ words but doesn't match name patterns - probably a title
        if len(author) > 3 and len(author.split()) >= 2:
            issues.append("not_a_name_pattern")

    # LastName, FirstName format
    if re.match(r'^[A-Z][a-z]+,\s+[A-Z][a-z]+', author):
        issues.append("lastname_firstname_format")

    # Format indicators
    if re.search(r'\.(epub|pdf|mp3|m4b)|(\[|\]|\{|\})', author, re.IGNORECASE):
        issues.append("format_junk_in_author")

    # Narrator included (usually with hyphen)
    if re.search(r'\s*-\s*[A-Z][a-z]+\s+[A-Z][a-z]+$', author):
        issues.append("possible_narrator_in_author")

    # Just numbers
    if re.match(r'^\d+$', author):
        issues.append("author_is_just_numbers")

    # Starts with number (might be book title)
    if re.match(r'^\d+\s', author):
        issues.append("author_starts_with_number")

    # Contains "Book N" or "Part N" - probably a title
    if re.search(r'\bbook\s*\d|\bpart\s*\d|\bvolume\s*\d', author, re.IGNORECASE):
        issues.append("author_contains_book_number")

    return issues


def analyze_title(title, author):
    """Analyze title for issues, return list of issues."""
    issues = []

    # Multi-book collection folder - these contain multiple books and need special handling
    # Don't process these as single books - they need to be split first
    # Be conservative - only flag patterns that DEFINITELY mean multiple books
    multi_book_patterns = [
        r'complete\s+series',           # "Complete Series"
        r'complete\s+audio\s+collection', # "Complete Audio Collection"
        r'\d+[-\s]?book\s+(set|box|collection)',  # "7-Book Set", "3 Book Collection"
        r'\d+[-\s]?book\s+and\s+audio',  # "7-Book and Audio Box Set"
        r'all\s+\d+\s+books',            # "All 9 Books"
        r'books?\s+\d+[-\s]?\d+',        # "Books 1-9", "Book 1-3"
    ]
    title_lower = title.lower()
    if any(re.search(p, title_lower) for p in multi_book_patterns):
        issues.append("multi_book_collection")
        return issues  # Don't bother with other checks - this needs manual handling

    # Author name repeated in title
    author_parts = author.lower().split()
    if len(author_parts) >= 2:
        if author.lower() in title.lower():
            issues.append("author_in_title")
        # Check for "by Author" pattern
        if re.search(rf'\bby\s+{re.escape(author)}\b', title, re.IGNORECASE):
            issues.append("by_author_in_title")

    # Year in title (but not book number like "1984")
    year_match = re.search(r'\(?(19[5-9][0-9]|20[0-2][0-9])\)?', title)
    if year_match:
        issues.append("year_in_title")

    # Quality/bitrate info
    if re.search(r'\d+k\b|\d+kbps|\d+mb|\d+gb', title, re.IGNORECASE):
        issues.append("quality_info_in_title")

    # Narrator name pattern (Name) at end
    if re.search(r'\([A-Z][a-z]+\)\s*$', title):
        issues.append("possible_narrator_in_title")

    # Duration pattern HH.MM.SS
    if re.search(r'\d{1,2}\.\d{2}\.\d{2}', title):
        issues.append("duration_in_title")

    # Series prefix like "Series Name Book 1 -"
    if re.search(r'^.+\s+book\s+\d+\s*[-:]\s*.+', title, re.IGNORECASE):
        issues.append("series_prefix_format")

    # Brackets with numbers (catalog IDs)
    if re.search(r'\[\d{4,}\]', title):
        issues.append("catalog_id_in_title")

    # Title looks like author name (just 2 capitalized words)
    title_words = title.split()
    if len(title_words) == 2 and all(w[0].isupper() for w in title_words if w):
        if not any(w.lower() in ['the', 'a', 'of', 'and'] for w in title_words):
            issues.append("title_looks_like_author")

    return issues


def find_audio_files(directory):
    """Recursively find all audio files in directory."""
    audio_files = []
    for root, dirs, files in os.walk(directory):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext in AUDIO_EXTENSIONS:
                audio_files.append(os.path.join(root, f))
    return audio_files


def find_ebook_files(directory):
    """Recursively find all ebook files in directory."""
    ebook_files = []
    for root, dirs, files in os.walk(directory):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext in EBOOK_EXTENSIONS:
                ebook_files.append(os.path.join(root, f))
    return ebook_files


def check_audio_file_health(file_path):
    """
    Check if an audio file is valid/corrupt using ffprobe.
    Returns dict with: valid (bool), duration (seconds or None), error (str or None)
    """
    import subprocess

    try:
        # First check: can ffprobe read duration?
        result = subprocess.run(
            ['ffprobe', '-i', file_path, '-show_entries', 'format=duration',
             '-v', 'quiet', '-of', 'csv=p=0'],
            capture_output=True, text=True, timeout=30
        )

        duration_str = result.stdout.strip()

        if not duration_str or duration_str == 'N/A':
            # Try to get more info about why it failed
            probe_result = subprocess.run(
                ['ffprobe', '-i', file_path, '-v', 'error'],
                capture_output=True, text=True, timeout=30
            )
            error_msg = probe_result.stderr.strip() or "Cannot read audio stream"
            return {'valid': False, 'duration': None, 'error': error_msg}

        try:
            duration = float(duration_str)
            # Sanity check - file should have reasonable duration
            if duration < 1:
                return {'valid': False, 'duration': duration, 'error': 'Duration too short (<1 sec)'}
            return {'valid': True, 'duration': duration, 'error': None}
        except ValueError:
            return {'valid': False, 'duration': None, 'error': f'Invalid duration: {duration_str}'}

    except subprocess.TimeoutExpired:
        return {'valid': False, 'duration': None, 'error': 'Timeout reading file'}
    except FileNotFoundError:
        return {'valid': False, 'duration': None, 'error': 'ffprobe not installed'}
    except Exception as e:
        return {'valid': False, 'duration': None, 'error': str(e)}


def get_file_signature(filepath, sample_size=8192):
    """Get a signature for duplicate detection (size + partial hash)."""
    try:
        size = os.path.getsize(filepath)
        with open(filepath, 'rb') as f:
            sample = f.read(sample_size)
        partial_hash = hashlib.md5(sample).hexdigest()[:16]
        return f"{size}_{partial_hash}"
    except:
        return None


def get_audio_fingerprint(filepath, duration=30, offset=0):
    """
    Get audio fingerprint using chromaprint/fpcalc.

    Args:
        filepath: Path to audio file
        duration: Seconds of audio to fingerprint (default 30)
        offset: Start position in seconds (for sampling middle/end)

    Returns dict with:
        - fingerprint: The chromaprint fingerprint string
        - duration: Total duration of the file in seconds
        - error: Error message if failed
    """
    import subprocess

    try:
        # Build fpcalc command
        cmd = ['fpcalc', '-length', str(duration)]
        if offset > 0:
            # Use ffmpeg to extract segment first (fpcalc doesn't support offset)
            # For now, we'll just fingerprint from the start
            pass
        cmd.append(str(filepath))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode != 0:
            return {'fingerprint': None, 'duration': None, 'error': result.stderr}

        # Parse output
        output = {}
        for line in result.stdout.strip().split('\n'):
            if '=' in line:
                key, value = line.split('=', 1)
                output[key] = value

        return {
            'fingerprint': output.get('FINGERPRINT'),
            'duration': int(output.get('DURATION', 0)),
            'error': None
        }
    except subprocess.TimeoutExpired:
        return {'fingerprint': None, 'duration': None, 'error': 'Timeout'}
    except FileNotFoundError:
        return {'fingerprint': None, 'duration': None, 'error': 'fpcalc not installed'}
    except Exception as e:
        return {'fingerprint': None, 'duration': None, 'error': str(e)}


def compare_fingerprints(fp1, fp2):
    """
    Compare two chromaprint fingerprints and return similarity score.

    Chromaprint fingerprints are base64-encoded integers that can be
    compared using popcount of XOR (Hamming distance).

    Returns similarity score 0.0 to 1.0
    """
    import base64
    import struct

    if not fp1 or not fp2:
        return 0.0

    try:
        # Decode base64 fingerprints to raw bytes
        raw1 = base64.b64decode(fp1 + '==')  # Add padding if needed
        raw2 = base64.b64decode(fp2 + '==')

        # Convert to list of 32-bit integers
        def to_ints(raw):
            # Skip first 4 bytes (header)
            data = raw[4:] if len(raw) > 4 else raw
            ints = []
            for i in range(0, len(data) - 3, 4):
                val = struct.unpack('<I', data[i:i+4])[0]
                ints.append(val)
            return ints

        ints1 = to_ints(raw1)
        ints2 = to_ints(raw2)

        if not ints1 or not ints2:
            return 0.0

        # Compare overlapping portion
        min_len = min(len(ints1), len(ints2))
        if min_len == 0:
            return 0.0

        # Count matching bits (using XOR and popcount)
        total_bits = min_len * 32
        different_bits = 0
        for i in range(min_len):
            xor = ints1[i] ^ ints2[i]
            different_bits += bin(xor).count('1')

        similarity = 1.0 - (different_bits / total_bits)
        return max(0.0, similarity)

    except Exception as e:
        logger.debug(f"Fingerprint comparison error: {e}")
        return 0.0


def analyze_audiobook_completeness(folder_path):
    """
    Analyze an audiobook folder for completeness.

    Returns dict with:
        - total_duration: Total duration in seconds
        - file_count: Number of audio files
        - files: List of {path, duration, fingerprint_start, fingerprint_end}
        - appears_complete: True if ending sounds like a proper ending
        - appears_partial: True if it seems cut off
    """
    folder = Path(folder_path)
    audio_files = sorted([f for f in folder.rglob('*') if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS])

    if not audio_files:
        return {'total_duration': 0, 'file_count': 0, 'files': [], 'appears_complete': False}

    files_info = []
    total_duration = 0

    for audio_file in audio_files:
        # Get fingerprint of first 30 seconds
        fp_start = get_audio_fingerprint(str(audio_file), duration=30)

        if fp_start['duration']:
            total_duration += fp_start['duration']
            files_info.append({
                'path': str(audio_file),
                'filename': audio_file.name,
                'duration': fp_start['duration'],
                'fingerprint_start': fp_start['fingerprint']
            })

    return {
        'total_duration': total_duration,
        'total_duration_hours': round(total_duration / 3600, 1),
        'file_count': len(audio_files),
        'files': files_info
    }


def compare_audiobooks_deep(source_path, dest_path):
    """
    Deep comparison of two audiobook folders using audio fingerprinting.

    This goes beyond file hashes to detect:
    - Same recording in different formats/bitrates
    - Partial copies (one is subset of other)
    - Different recordings of same book (different narrators)
    - Corrupt/unreadable files

    Returns dict with:
        - same_recording: True if audio content matches
        - source_is_subset: True if source is partial copy of dest
        - dest_is_subset: True if dest is partial copy of source
        - recording_similarity: 0.0-1.0 how similar the recordings are
        - recommendation: 'keep_source', 'keep_dest', 'keep_both', 'merge'
        - source_corrupt: True if source files are corrupt/unreadable
        - dest_corrupt: True if dest files are corrupt/unreadable
    """
    source_info = analyze_audiobook_completeness(source_path)
    dest_info = analyze_audiobook_completeness(dest_path)

    # Check for corrupt files (files exist but can't be read/have no duration)
    source_corrupt = source_info['file_count'] > 0 and source_info['total_duration'] == 0
    dest_corrupt = dest_info['file_count'] > 0 and dest_info['total_duration'] == 0

    result = {
        'source_duration': source_info['total_duration'],
        'source_duration_hours': source_info.get('total_duration_hours', 0),
        'dest_duration': dest_info['total_duration'],
        'dest_duration_hours': dest_info.get('total_duration_hours', 0),
        'source_files': source_info['file_count'],
        'dest_files': dest_info['file_count'],
        'source_readable_files': len(source_info['files']),
        'dest_readable_files': len(dest_info['files']),
        'same_recording': False,
        'source_is_subset': False,
        'dest_is_subset': False,
        'recording_similarity': 0.0,
        'source_corrupt': source_corrupt,
        'dest_corrupt': dest_corrupt,
        'recommendation': 'keep_both'
    }

    # Handle corrupt files
    if source_corrupt and not dest_corrupt:
        result['recommendation'] = 'keep_dest'
        result['reason'] = 'Source files are corrupt/unreadable'
        return result
    elif dest_corrupt and not source_corrupt:
        result['recommendation'] = 'keep_source'
        result['reason'] = 'Destination files are corrupt/unreadable'
        return result
    elif source_corrupt and dest_corrupt:
        result['recommendation'] = 'keep_both'
        result['reason'] = 'Both versions have corrupt/unreadable files'
        return result

    if not source_info['files'] or not dest_info['files']:
        return result

    # Compare first file fingerprints to detect same recording
    source_first_fp = source_info['files'][0].get('fingerprint_start')
    dest_first_fp = dest_info['files'][0].get('fingerprint_start')

    if source_first_fp and dest_first_fp:
        similarity = compare_fingerprints(source_first_fp, dest_first_fp)
        result['recording_similarity'] = round(similarity, 2)

        # High similarity = same recording
        if similarity >= 0.7:
            result['same_recording'] = True

            # Determine which is more complete
            source_dur = source_info['total_duration']
            dest_dur = dest_info['total_duration']

            if source_dur > dest_dur * 1.1:  # Source is 10%+ longer
                result['dest_is_subset'] = True
                result['recommendation'] = 'keep_source'
                result['reason'] = f'Source is more complete ({source_dur//60}min vs {dest_dur//60}min)'
            elif dest_dur > source_dur * 1.1:  # Dest is 10%+ longer
                result['source_is_subset'] = True
                result['recommendation'] = 'keep_dest'
                result['reason'] = f'Destination is more complete ({dest_dur//60}min vs {source_dur//60}min)'
            else:
                # Similar length - prefer the properly named one (dest)
                result['recommendation'] = 'keep_dest'
                result['reason'] = 'Same recording, similar length - keeping properly named version'
        else:
            # Different recordings
            result['reason'] = f'Different recordings ({similarity:.0%} similarity)'

    return result


def compare_book_folders(source_path, dest_path, deep_analysis=True):
    """
    Compare two book folders to determine if they contain the same audiobook.

    Args:
        source_path: Path to source folder
        dest_path: Path to destination folder
        deep_analysis: If True, use audio fingerprinting for comparison (slower but more accurate)

    Returns dict with:
        - identical: True if folders contain the same audio files
        - source_only: Files only in source
        - dest_only: Files only in destination
        - matching: Files that match between both
        - source_better: True if source has more/better files
        - dest_better: True if destination has more/better files
        - deep_analysis: Results from audio fingerprint comparison (if enabled)
    """
    source = Path(source_path)
    dest = Path(dest_path)

    # Get audio files from both folders
    source_audio = [f for f in source.rglob('*') if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
    dest_audio = [f for f in dest.rglob('*') if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]

    # Build signature maps
    source_sigs = {}
    for f in source_audio:
        sig = get_file_signature(str(f))
        if sig:
            source_sigs[sig] = f

    dest_sigs = {}
    for f in dest_audio:
        sig = get_file_signature(str(f))
        if sig:
            dest_sigs[sig] = f

    # Compare
    source_sig_set = set(source_sigs.keys())
    dest_sig_set = set(dest_sigs.keys())

    matching = source_sig_set & dest_sig_set
    source_only = source_sig_set - dest_sig_set
    dest_only = dest_sig_set - source_sig_set

    # Calculate total sizes
    source_total_size = sum(os.path.getsize(str(f)) for f in source_audio)
    dest_total_size = sum(os.path.getsize(str(f)) for f in dest_audio)

    result = {
        'identical': len(matching) > 0 and len(source_only) == 0 and len(dest_only) == 0,
        'matching_count': len(matching),
        'source_only_count': len(source_only),
        'dest_only_count': len(dest_only),
        'source_files': len(source_audio),
        'dest_files': len(dest_audio),
        'source_size': source_total_size,
        'dest_size': dest_total_size,
        'source_better': source_total_size > dest_total_size or len(source_audio) > len(dest_audio),
        'dest_better': dest_total_size > source_total_size or len(dest_audio) > len(source_audio),
        'overlap_ratio': len(matching) / max(len(source_sig_set), len(dest_sig_set), 1)
    }

    # Determine if it's effectively the same book (high overlap or one is subset of other)
    if result['overlap_ratio'] >= 0.8:
        result['same_book'] = True
    elif len(matching) > 0 and (len(source_only) == 0 or len(dest_only) == 0):
        # One is a subset of the other
        result['same_book'] = True
    else:
        result['same_book'] = False

    # Deep analysis using audio fingerprinting (detects same recording in different formats,
    # corrupt files, partial copies)
    if deep_analysis and not result['same_book']:
        try:
            deep_result = compare_audiobooks_deep(source_path, dest_path)
            result['deep_analysis'] = deep_result

            # Update same_book based on deep analysis
            if deep_result.get('same_recording'):
                result['same_book'] = True
                result['same_recording'] = True

            # Update better flags based on corrupt file detection
            if deep_result.get('dest_corrupt') and not deep_result.get('source_corrupt'):
                result['source_better'] = True
                result['dest_better'] = False
                result['dest_corrupt'] = True
            elif deep_result.get('source_corrupt') and not deep_result.get('dest_corrupt'):
                result['source_better'] = False
                result['dest_better'] = True
                result['source_corrupt'] = True

            # Copy recommendation
            result['recommendation'] = deep_result.get('recommendation', 'keep_both')
            result['reason'] = deep_result.get('reason', '')

        except Exception as e:
            logger.debug(f"Deep analysis failed: {e}")
            result['deep_analysis'] = None

    return result


def deep_scan_library(config):
    """
    Deep scan library - the AUTISTIC LIBRARIAN approach.
    Finds ALL issues, duplicates, and structural problems.
    """
    conn = get_db()
    c = conn.cursor()

    checked = 0  # Total book folders examined
    scanned = 0  # New books added to tracking
    queued = 0   # Books added to fix queue
    issues_found = {}  # path -> list of issues

    # Track files for duplicate detection
    file_signatures = {}  # signature -> list of paths
    file_names = {}  # basename -> list of paths

    logger.info("=== DEEP LIBRARY SCAN STARTING ===")

    for lib_path_str in config.get('library_paths', []):
        lib_path = Path(lib_path_str)
        if not lib_path.exists():
            logger.warning(f"Library path not found: {lib_path}")
            continue

        logger.info(f"Scanning: {lib_path}")

        # First pass: Find all audio files to understand actual book locations
        all_audio_files = find_audio_files(lib_path)
        logger.info(f"Found {len(all_audio_files)} audio files")

        # Track file signatures for duplicate detection
        for audio_file in all_audio_files:
            sig = get_file_signature(audio_file)
            if sig:
                if sig not in file_signatures:
                    file_signatures[sig] = []
                file_signatures[sig].append(audio_file)

            basename = os.path.basename(audio_file).lower()
            if basename not in file_names:
                file_names[basename] = []
            file_names[basename].append(audio_file)

        # NEW: Detect loose files in library root (no folder structure)
        loose_files = []
        for item in lib_path.iterdir():
            if item.is_file() and item.suffix.lower() in AUDIO_EXTENSIONS:
                loose_files.append(item)

        if loose_files:
            logger.info(f"Found {len(loose_files)} loose audio files in library root")
            for loose_file in loose_files:
                # Parse filename to extract searchable title
                filename = loose_file.stem  # filename without extension
                cleaned_filename = clean_search_title(filename)
                path_str = str(loose_file)

                # Check if already in books table
                c.execute('SELECT id FROM books WHERE path = ?', (path_str,))
                existing = c.fetchone()

                if existing:
                    book_id = existing['id']
                else:
                    # Create books record for the loose file
                    c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                VALUES (?, ?, ?, ?)''',
                             (path_str, 'Unknown', cleaned_filename, 'loose_file'))
                    book_id = c.lastrowid

                # Add to queue with special "loose_file" reason
                c.execute('''INSERT OR REPLACE INTO queue
                            (book_id, reason, added_at, priority)
                            VALUES (?, ?, ?, ?)''',
                         (book_id, f'loose_file_needs_folder:{filename}',
                          datetime.now().isoformat(), 1))  # High priority
                # Set verification_layer=1 to start at Layer 1 (API lookup)
                c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                queued += 1
                issues_found[path_str] = ['loose_file_no_folder']
                logger.info(f"Queued loose file: {filename} -> search for: {cleaned_filename}")

        # NEW: Detect loose EBOOK files in library root (when ebook management enabled)
        if config.get('ebook_management', False):
            loose_ebooks = []
            for item in lib_path.iterdir():
                if item.is_file() and item.suffix.lower() in EBOOK_EXTENSIONS:
                    loose_ebooks.append(item)

            if loose_ebooks:
                logger.info(f"Found {len(loose_ebooks)} loose ebook files in library root")
                for loose_ebook in loose_ebooks:
                    filename = loose_ebook.stem
                    cleaned_filename = clean_search_title(filename)
                    path_str = str(loose_ebook)

                    c.execute('SELECT id FROM books WHERE path = ?', (path_str,))
                    existing = c.fetchone()

                    if existing:
                        book_id = existing['id']
                    else:
                        c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                    VALUES (?, ?, ?, ?)''',
                                 (path_str, 'Unknown', cleaned_filename, 'ebook_loose'))
                        book_id = c.lastrowid

                    c.execute('''INSERT OR REPLACE INTO queue
                                (book_id, reason, added_at, priority)
                                VALUES (?, ?, ?, ?)''',
                             (book_id, f'ebook_loose:{filename}',
                              datetime.now().isoformat(), 2))
                    # Set verification_layer=1 to start at Layer 1 (API lookup)
                    c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                    queued += 1
                    issues_found[path_str] = ['ebook_loose_file']
                    logger.info(f"Queued loose ebook: {filename}")

        # Second pass: Analyze folder structure
        for author_dir in lib_path.iterdir():
            if not author_dir.is_dir():
                continue

            author = author_dir.name

            # Skip system folders at author level - these are NEVER authors
            author_system_folders = {'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
                                     'backup', 'backups', 'old', 'new', 'test', 'tests', 'sample', 'samples',
                                     '.thumbnails', 'thumbnails', 'covers', 'images', 'artwork', 'art',
                                     'streams', '.streams', '.cache', '.metadata', '@eaDir', '#recycle'}
            if author.lower() in author_system_folders or author.startswith('.') or author.startswith('@'):
                logger.debug(f"Skipping system folder at author level: {author}")
                continue

            author_issues = analyze_author(author)

            # Check if "author" folder is actually a book (has audio files directly)
            direct_audio = [f for f in author_dir.iterdir()
                          if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
            if direct_audio:
                # This "author" folder might actually be a book!
                issues_found[str(author_dir)] = author_issues + ["author_folder_has_audio_files"]
                logger.warning(f"Author folder has audio files directly: {author}")

            # Check if author folder has NO book subfolders (just disc folders)
            subdirs = [d for d in author_dir.iterdir() if d.is_dir()]
            if subdirs:
                all_disc_folders = all(is_disc_chapter_folder(d.name) for d in subdirs)
                if all_disc_folders:
                    issues_found[str(author_dir)] = author_issues + ["author_folder_only_has_disc_folders"]

            for title_dir in author_dir.iterdir():
                if not title_dir.is_dir():
                    continue

                title = title_dir.name
                path = str(title_dir)

                # Skip if this looks like a disc/chapter folder
                if is_disc_chapter_folder(title):
                    # But flag the parent!
                    issues_found[str(author_dir)] = issues_found.get(str(author_dir), []) + [f"has_disc_folder:{title}"]
                    continue

                # Skip system/metadata folders - these are NEVER books
                system_folders = {'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
                                  'backup', 'backups', 'old', 'new', 'test', 'tests', 'sample', 'samples',
                                  '.thumbnails', 'thumbnails', 'covers', 'images', 'artwork', 'art',
                                  'extras', 'bonus', 'misc', 'other', 'various', 'unknown', 'unsorted',
                                  'downloads', 'incoming', 'processing', 'completed', 'done', 'failed',
                                  'streams', 'chapters', 'parts', '.streams', '.cache', '.metadata'}
                if title.lower() in system_folders or title.startswith('.'):
                    logger.debug(f"Skipping system folder: {path}")
                    continue

                # Check if this is a SERIES folder containing multiple book subfolders
                # If so, skip it - we should process the books inside, not the series folder itself
                subdirs = [d for d in title_dir.iterdir() if d.is_dir()]
                if len(subdirs) >= 2:
                    # Count how many look like book folders (numbered, "Book N", etc.)
                    book_folder_patterns = [
                        r'^\d+\s*[-–—:.]?\s*\w',     # "01 Title", "1 - Title", "01. Title"
                        r'^#?\d+\s*[-–—:]',          # "#1 - Title"
                        r'book\s*\d+',               # "Book 1", "Book1"
                        r'vol(ume)?\s*\d+',          # "Volume 1", "Vol 1"
                        r'part\s*\d+',               # "Part 1"
                    ]
                    book_like_count = sum(
                        1 for d in subdirs
                        if any(re.search(p, d.name, re.IGNORECASE) for p in book_folder_patterns)
                    )
                    if book_like_count >= 2:
                        # This is a series folder, not a book - skip it
                        logger.info(f"Skipping series folder (contains {book_like_count} book subfolders): {path}")
                        # Mark in database as series_folder so we don't keep checking it
                        c.execute('SELECT id FROM books WHERE path = ?', (path,))
                        existing = c.fetchone()
                        if existing:
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('series_folder', existing['id']))
                        else:
                            c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                         VALUES (?, ?, ?, 'series_folder')''', (path, author, title))
                        conn.commit()
                        continue

                # Check if this folder contains multiple AUDIO FILES that look like different books
                # (e.g., "Book 1.m4b", "Book 2.m4b" or "Necroscope Book 1.m4b", "Necroscope Book 2.m4b")
                # Issue #29 fix: Use smart detection to avoid false positives on chapter files
                audio_files = [f for f in title_dir.iterdir()
                               if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
                if len(audio_files) >= 2:
                    # Use smart multibook detection instead of brittle regex
                    multibook_result = detect_multibook_vs_chapters(audio_files, config)

                    if multibook_result['is_multibook']:
                        # Confirmed multi-book collection - skip it
                        logger.info(f"Skipping multi-book collection ({multibook_result['reason']}): {path}")
                        c.execute('SELECT id FROM books WHERE path = ?', (path,))
                        existing = c.fetchone()
                        if existing:
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('multi_book_files', existing['id']))
                        else:
                            c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                         VALUES (?, ?, ?, 'multi_book_files')''', (path, author, title))
                        conn.commit()
                        continue

                # This is a valid book folder - count it
                checked += 1

                # Analyze title
                title_issues = analyze_title(title, author)
                cleaned_title, clean_issues = clean_title(title)

                all_issues = author_issues + title_issues + clean_issues

                # CRITICAL: Detect REVERSED STRUCTURE (Series/Author instead of Author/Series)
                # When: author folder looks like a title AND title folder looks like an author
                author_looks_like_title = any(i in author_issues for i in [
                    'year_in_author', 'title_words_in_author', 'author_contains_book_number',
                    'not_a_name_pattern', 'author_starts_with_number'
                ])
                title_looks_like_author = 'title_looks_like_author' in title_issues

                # Check if title folder is a proper name pattern (First Last)
                title_is_name_pattern = bool(re.match(
                    r'^[A-Z][a-z]+\s+[A-Z][a-z]+$|^[A-Z]\.\s*[A-Z][a-z]+$|^[A-Z][a-z]+,\s+[A-Z]',
                    title
                ))

                if author_looks_like_title and (title_looks_like_author or title_is_name_pattern):
                    # This is a reversed structure! Mark it specially
                    all_issues = ['STRUCTURE_REVERSED'] + all_issues
                    logger.info(f"Detected reversed structure: '{author}' is title, '{title}' is author")

                    # Set status to 'structure_reversed' so we handle it differently
                    c.execute('SELECT id FROM books WHERE path = ?', (path,))
                    existing_rev = c.fetchone()
                    if existing_rev:
                        c.execute('UPDATE books SET status = ? WHERE id = ?',
                                  ('structure_reversed', existing_rev['id']))
                    else:
                        c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                     VALUES (?, ?, ?, 'structure_reversed')''', (path, author, title))
                    conn.commit()
                    # Don't add to regular queue - needs special handling
                    continue

                # Check for nested structure (disc folders inside book folder)
                nested_dirs = [d for d in title_dir.iterdir() if d.is_dir()]
                disc_dirs = [d for d in nested_dirs if is_disc_chapter_folder(d.name)]
                if disc_dirs:
                    all_issues.append(f"has_{len(disc_dirs)}_disc_folders")

                # Check for ebook files
                ebook_files = [f for f in title_dir.rglob('*') if f.suffix.lower() in EBOOK_EXTENSIONS]
                audio_in_folder = [f for f in title_dir.rglob('*') if f.suffix.lower() in AUDIO_EXTENSIONS]

                if ebook_files:
                    if audio_in_folder:
                        # Mixed folder - ebooks with audiobooks
                        all_issues.append(f"has_{len(ebook_files)}_ebook_files")
                    elif config.get('ebook_management', False):
                        # Ebook-only folder - queue for ebook organization
                        all_issues.append('ebook_only_folder')
                        logger.info(f"Found ebook-only folder: {path} ({len(ebook_files)} ebooks)")

                # Store issues
                if all_issues:
                    issues_found[path] = all_issues

                # Add to database
                c.execute('SELECT id, status FROM books WHERE path = ?', (path,))
                existing = c.fetchone()

                if existing:
                    if existing['status'] in ['verified', 'fixed']:
                        continue
                    book_id = existing['id']
                else:
                    c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                 VALUES (?, ?, ?, 'pending')''', (path, author, title))
                    conn.commit()
                    book_id = c.lastrowid
                    scanned += 1

                # Add to queue if has issues
                if all_issues:
                    # Skip multi-book collections - they need manual splitting, not renaming
                    if 'multi_book_collection' in all_issues:
                        logger.info(f"Skipping multi-book collection (needs manual split): {path}")
                        c.execute('UPDATE books SET status = ? WHERE id = ?',
                                  ('needs_split', book_id))
                        conn.commit()
                        continue

                    reason = "; ".join(all_issues[:3])  # First 3 issues
                    if len(all_issues) > 3:
                        reason += f" (+{len(all_issues)-3} more)"

                    c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                    if not c.fetchone():
                        c.execute('''INSERT INTO queue (book_id, reason, priority)
                                    VALUES (?, ?, ?)''',
                                 (book_id, reason, min(len(all_issues), 10)))
                        # Set verification_layer=1 to start at Layer 1 (API lookup)
                        c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                        conn.commit()
                        queued += 1

    # Third pass: Flag duplicates
    logger.info("Checking for duplicates...")
    duplicate_count = 0

    for sig, paths in file_signatures.items():
        if len(paths) > 1:
            duplicate_count += 1
            for p in paths:
                book_dir = str(Path(p).parent)
                if book_dir in issues_found:
                    issues_found[book_dir].append(f"duplicate_file:{os.path.basename(p)}")
                else:
                    issues_found[book_dir] = [f"duplicate_file:{os.path.basename(p)}"]

    logger.info(f"Found {duplicate_count} potential duplicate file sets")

    # Update daily stats (INSERT if not exists, then UPDATE to preserve other columns)
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('''UPDATE stats SET
                 scanned = COALESCE(scanned, 0) + ?,
                 queued = COALESCE(queued, 0) + ?
                 WHERE date = ?''', (scanned, queued, today))
    conn.commit()
    conn.close()

    logger.info(f"=== DEEP SCAN COMPLETE ===")
    logger.info(f"Checked: {checked} book folders")
    logger.info(f"Scanned: {scanned} new books added to tracking")
    logger.info(f"Queued: {queued} books need fixing")
    logger.info(f"Already correct: {checked - queued} books")

    return checked, scanned, queued


def scan_library(config):
    """Wrapper that calls deep scan."""
    return deep_scan_library(config)

def check_rate_limit(config):
    """Check if we're within API rate limits. Returns (allowed, calls_this_hour, limit)."""
    conn = get_db()
    c = conn.cursor()

    max_per_hour = config.get('max_requests_per_hour', 30)

    # Get calls in the last hour
    one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
    today = datetime.now().strftime('%Y-%m-%d')

    c.execute('SELECT api_calls FROM stats WHERE date = ?', (today,))
    row = c.fetchone()
    calls_today = row['api_calls'] if row else 0

    conn.close()

    # Simple hourly check - in practice we're using daily count as approximation
    # For more accurate tracking, we'd need a separate API call log table
    allowed = calls_today < max_per_hour
    return allowed, calls_today, max_per_hour


def process_layer_1_api(config, limit=None):
    """
    Layer 1: API Database Lookups

    Processes items at verification_layer=1 using API databases (BookDB, Audnexus, etc.)
    Items that get a confident match are marked complete.
    Items that fail are advanced to layer 2 (AI verification).

    This is faster and cheaper than AI, so we try it first.
    """
    if not config.get('enable_api_lookups', True):
        logger.info("[LAYER 1] API lookups disabled, skipping")
        return 0, 0

    conn = get_db()
    c = conn.cursor()

    batch_size = limit or config.get('batch_size', 3)
    confidence_threshold = config.get('profile_confidence_threshold', 85)

    # Get items awaiting API lookup (layer 1) or new items (layer 0)
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title, b.verification_layer
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.verification_layer IN (0, 1)
                   AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (batch_size,))
    batch = c.fetchall()

    if not batch:
        conn.close()
        return 0, 0

    logger.info(f"[LAYER 1] Processing {len(batch)} items via API lookup")

    processed = 0
    resolved = 0

    for row in batch:
        path = row['path']
        current_author = row['current_author']
        current_title = row['current_title']

        # Use existing API candidate gathering function
        candidates = gather_all_api_candidates(current_title, current_author, config)

        if candidates:
            # Sort by author match quality - prefer exact matches
            best_match = None
            for candidate in candidates:
                cand_author = (candidate.get('author') or '').lower()
                if current_author.lower() == cand_author or is_placeholder_author(current_author):
                    best_match = candidate
                    break

            if not best_match:
                best_match = candidates[0]  # Take first match as fallback

            # Check if this is a good enough match
            match_title = best_match.get('title', '')
            match_author = best_match.get('author', '')

            if match_title and match_author:
                # Calculate match confidence using word overlap similarity (returns 0.0-1.0)
                title_sim = calculate_title_similarity(current_title, match_title) if current_title else 0

                # IMPORTANT: If author is placeholder (Unknown, Various, etc.), we CANNOT verify as-is
                # The book needs to be fixed, not verified. Advance to Layer 2 for proper identification.
                if is_placeholder_author(current_author):
                    logger.info(f"[LAYER 1] Placeholder author '{current_author}', advancing to AI for identification: {current_title}")
                    c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
                    processed += 1
                    continue

                author_sim = calculate_title_similarity(current_author, match_author)
                avg_confidence = (title_sim + author_sim) / 2

                # confidence_threshold is 0-100 scale, convert to 0-1
                threshold = confidence_threshold / 100.0 if confidence_threshold > 1 else confidence_threshold
                if avg_confidence >= threshold:
                    # Good match found - check if current values are correct or need fixing
                    # If both title and author are very close (90%+), the book is already correct
                    # If one differs significantly, advance to Layer 2 for AI verification

                    if title_sim >= 0.90 and author_sim >= 0.90:
                        # Book is already correctly named - mark as verified and remove from queue
                        logger.info(f"[LAYER 1] Verified OK ({avg_confidence:.0%}): {current_author}/{current_title}")
                        c.execute('UPDATE books SET status = ?, verification_layer = 4 WHERE id = ?',
                                 ('verified', row['book_id']))
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        resolved += 1
                    else:
                        # API found the book but current values differ - let Layer 2 handle the fix
                        logger.info(f"[LAYER 1] API match needs fix ({avg_confidence:.0%}, title={title_sim:.0%}, author={author_sim:.0%}): {current_author}/{current_title} -> {match_author}/{match_title}")
                        c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
                else:
                    # Low confidence - advance to AI layer
                    logger.info(f"[LAYER 1] API match low confidence ({avg_confidence:.0f}%), advancing to AI: {current_author}/{current_title}")
                    c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
            else:
                # No good match found - advance to AI
                logger.info(f"[LAYER 1] No API match, advancing to AI: {current_author}/{current_title}")
                c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
        else:
            # No candidates at all - advance to AI
            logger.info(f"[LAYER 1] No API candidates, advancing to AI: {current_author}/{current_title}")
            c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))

        processed += 1

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 1] Processed {processed}, resolved {resolved} via API")
    return processed, resolved


def process_queue(config, limit=None):
    """Process items in the queue."""
    # Check rate limit first
    allowed, calls_made, max_calls = check_rate_limit(config)
    if not allowed:
        logger.warning(f"Rate limit reached: {calls_made}/{max_calls} calls. Waiting...")
        return 0, 0

    conn = get_db()
    c = conn.cursor()

    batch_size = config.get('batch_size', 3)
    if limit:
        batch_size = min(batch_size, limit)

    logger.info(f"[LAYER 2/AI] process_queue called with batch_size={batch_size}, limit={limit} (API: {calls_made}/{max_calls})")

    # Check if AI verification is enabled
    if not config.get('enable_ai_verification', True):
        logger.info("[LAYER 2] AI verification disabled, skipping")
        conn.close()
        return 0, 0

    # Get batch from queue - LAYER 2 (AI): items at verification_layer=2 or legacy items (layer=0 when API is disabled)
    # Also process layer=0 items if API lookups are disabled (fallback to AI directly)
    api_enabled = config.get('enable_api_lookups', True)
    if api_enabled:
        # Only process items that passed through Layer 1 (API)
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.verification_layer = 2
                       AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                     ORDER BY q.priority, q.added_at
                     LIMIT ?''', (batch_size,))
    else:
        # API disabled - process all queue items directly with AI
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                     ORDER BY q.priority, q.added_at
                     LIMIT ?''', (batch_size,))
    batch = c.fetchall()

    logger.info(f"[LAYER 2/AI] Fetched {len(batch)} items from queue")

    if not batch:
        logger.info("[DEBUG] No items in batch, returning 0")
        conn.close()
        return 0, 0  # (processed, fixed)

    # Build messy names for AI
    messy_names = [f"{row['current_author']} - {row['current_title']}" for row in batch]

    logger.info(f"[DEBUG] Processing batch of {len(batch)} items:")
    for i, name in enumerate(messy_names):
        logger.info(f"[DEBUG]   Item {i+1}: {name}")

    results = call_ai(messy_names, config)
    logger.info(f"[DEBUG] AI returned {len(results) if results else 0} results")

    # Update API call stats (INSERT if not exists, then UPDATE to preserve other columns)
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('UPDATE stats SET api_calls = COALESCE(api_calls, 0) + 1 WHERE date = ?', (today,))

    if not results:
        logger.warning("No results from AI")
        conn.commit()
        conn.close()
        return 0, 0  # (processed, fixed)

    processed = 0
    fixed = 0
    for row, result in zip(batch, results):
        # SAFETY CHECK: Before processing, verify this isn't a multi-book collection
        # that slipped through (items already in queue before detection was added)
        old_path = Path(row['path'])
        if old_path.exists() and old_path.is_dir():
            # Check for multiple book SUBFOLDERS
            subdirs = [d for d in old_path.iterdir() if d.is_dir()]
            if len(subdirs) >= 2:
                book_folder_patterns = [
                    r'^\d+\s*[-–—:.]?\s*\w', r'^#?\d+\s*[-–—:]',
                    r'book\s*\d+', r'vol(ume)?\s*\d+', r'part\s*\d+'
                ]
                book_like_count = sum(1 for d in subdirs
                    if any(re.search(p, d.name, re.IGNORECASE) for p in book_folder_patterns))
                if book_like_count >= 2:
                    logger.warning(f"BLOCKED: {row['path']} is a series folder ({book_like_count} book subfolders) - skipping")
                    c.execute('UPDATE books SET status = ? WHERE id = ?', ('series_folder', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    processed += 1
                    continue

            # Check for multiple book FILES using smart detection (Issue #29 fix)
            audio_files = [f for f in old_path.iterdir()
                           if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
            if len(audio_files) >= 2:
                multibook_result = detect_multibook_vs_chapters(audio_files, config)
                if multibook_result['is_multibook']:
                    logger.warning(f"BLOCKED: {row['path']} is multibook ({multibook_result['reason']}) - skipping")
                    c.execute('UPDATE books SET status = ? WHERE id = ?', ('multi_book_files', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    processed += 1
                    continue

        new_author = (result.get('author') or '').strip()
        new_title = (result.get('title') or '').strip()
        new_narrator = (result.get('narrator') or '').strip() or None  # None if empty
        new_series = (result.get('series') or '').strip() or None  # Series name
        new_series_num = result.get('series_num')  # Series number (can be int or string like "1" or "Book 1")
        new_year = result.get('year')  # Publication year
        new_edition = (result.get('edition') or '').strip() or None  # Anniversary, Unabridged, etc.
        new_variant = (result.get('variant') or '').strip() or None  # Graphic Audio, Full Cast, BBC Radio

        # If AI didn't detect series, try to extract it from title patterns
        # First try the ORIGINAL title (has series info like "The Reckoners, Book 2 - Firefight")
        # Then try the new title as fallback
        if not new_series:
            # Try original title first (most likely to have series pattern)
            original_title = row['current_title']
            extracted_series, extracted_num, extracted_title = extract_series_from_title(original_title)
            if extracted_series:
                new_series = extracted_series
                new_series_num = extracted_num
                # Keep the AI's cleaned title, just add the series info
                logger.info(f"Extracted series from original title: '{extracted_series}' #{extracted_num}")
            else:
                # Got book number but no series name? Check if original "author" is actually a series
                if extracted_num and not new_series:
                    original_author = row['current_author']
                    # Check if original author looks like a series name
                    series_indicators = ['series', 'saga', 'cycle', 'chronicles', 'trilogy', 'collection',
                                         'edition', 'novels', 'books', 'tales', 'adventures', 'mysteries']
                    if any(ind in original_author.lower() for ind in series_indicators):
                        new_series = original_author
                        new_series_num = extracted_num
                        logger.info(f"Using original author as series: '{new_series}' #{new_series_num}")

            # Fallback: try the new title
            if not new_series and new_title:
                extracted_series, extracted_num, extracted_title = extract_series_from_title(new_title)
                if extracted_series:
                    new_series = extracted_series
                    new_series_num = extracted_num
                    new_title = extracted_title
                    logger.info(f"Extracted series from new title: '{extracted_series}' #{extracted_num} - '{extracted_title}'")

        if not new_author or not new_title:
            # Remove from queue, mark as verified
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
            processed += 1
            logger.info(f"Verified OK (empty result): {row['current_author']}/{row['current_title']}")
            continue

        # Check if fix needed (also check narrator change)
        if new_author != row['current_author'] or new_title != row['current_title'] or new_narrator:
            old_path = Path(row['path'])

            # Find which configured library this book belongs to
            # (Don't assume 2-level structure - series_grouping uses 3 levels)
            lib_path = None
            for lp in config.get('library_paths', []):
                lp_path = Path(lp)
                try:
                    old_path.relative_to(lp_path)
                    lib_path = lp_path
                    break
                except ValueError:
                    continue

            # Fallback if not found in configured libraries
            if lib_path is None:
                lib_path = old_path.parent.parent
                logger.warning(f"Book path {old_path} not under any configured library, guessing lib_path={lib_path}")

            new_path = build_new_path(lib_path, new_author, new_title,
                                      series=new_series, series_num=new_series_num,
                                      narrator=new_narrator, year=new_year,
                                      edition=new_edition, variant=new_variant, config=config)

            # For loose files, new_path should include the filename
            is_loose_file = row['reason'] and row['reason'].startswith('loose_file_needs_folder')
            if is_loose_file and old_path.is_file():
                # Append original filename to the new folder path
                new_path = new_path / old_path.name
                logger.info(f"Loose file: will move {old_path.name} to {new_path}")

            # For loose ebook files
            is_loose_ebook = row['reason'] and row['reason'].startswith('ebook_loose')
            if is_loose_ebook and old_path.is_file():
                ebook_mode = config.get('ebook_library_mode', 'merge')
                if ebook_mode == 'merge':
                    # Look for existing audiobook folder to merge into
                    safe_author = sanitize_path_component(new_author)
                    safe_title = sanitize_path_component(new_title)
                    potential_audiobook_path = lib_path / safe_author / safe_title
                    if potential_audiobook_path.exists():
                        # Found matching audiobook folder - put ebook there
                        new_path = potential_audiobook_path / old_path.name
                        logger.info(f"Ebook merge: found audiobook folder, moving to {new_path}")
                    else:
                        # No audiobook folder - create ebook folder like normal
                        new_path = new_path / old_path.name
                        logger.info(f"Ebook: no audiobook folder found, creating new at {new_path}")
                else:
                    # Separate mode - create ebook folder
                    new_path = new_path / old_path.name
                    logger.info(f"Ebook: separate mode, moving to {new_path}")

            # CRITICAL SAFETY: If path building failed, skip this item
            if new_path is None:
                logger.error(f"SAFETY BLOCK: Invalid path for '{new_author}' / '{new_title}' - skipping to prevent data loss")
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                         ('error', 'Path validation failed - unsafe author/title', row['book_id']))
                conn.commit()
                processed += 1
                continue

            # Check for drastic author change
            drastic_change = is_drastic_author_change(row['current_author'], new_author)
            protect_authors = config.get('protect_author_changes', True)

            # If drastic change detected, run verification pipeline
            if drastic_change and protect_authors:
                logger.info(f"DRASTIC CHANGE DETECTED: {row['current_author']} -> {new_author}, running verification...")

                # Run verification with all APIs
                original_input = f"{row['current_author']}/{row['current_title']}"
                verification = verify_drastic_change(
                    original_input,
                    row['current_author'], row['current_title'],
                    new_author, new_title,
                    config
                )

                if verification:
                    if verification['verified']:
                        # AI verified the change is correct (or corrected it)
                        new_author = verification['author']
                        new_title = verification['title']
                        # Recheck if it's still drastic after verification
                        drastic_change = is_drastic_author_change(row['current_author'], new_author)
                        logger.info(f"VERIFIED: {row['current_author']} -> {new_author} ({verification['reasoning'][:50]}...)")
                    elif verification['decision'] == 'WRONG':
                        # AI says the change is wrong - use the recommended fix instead
                        new_author = verification['author']
                        new_title = verification['title']
                        drastic_change = is_drastic_author_change(row['current_author'], new_author)
                        logger.info(f"CORRECTED: {row['current_author']} -> {new_author} (was wrong: {verification['reasoning'][:50]}...)")
                    else:
                        # AI is uncertain - check if Trust the Process mode enabled
                        trust_mode = config.get('trust_the_process', False)
                        if trust_mode and config.get('gemini_api_key'):
                            # Try audio analysis as tie-breaker
                            logger.info(f"TRUST THE PROCESS: Uncertain verification, trying audio tie-breaker...")
                            audio_result = None
                            audio_files = find_audio_files(str(old_path))
                            if audio_files:
                                audio_result = analyze_audio_with_gemini(audio_files[0], config)

                            if audio_result and audio_result.get('author'):
                                audio_author = audio_result.get('author', '')
                                audio_title = audio_result.get('title', '')
                                # Check if audio confirms new author OR original author
                                new_match = audio_author.lower() in new_author.lower() or new_author.lower() in audio_author.lower()
                                old_match = audio_author.lower() in row['current_author'].lower() or row['current_author'].lower() in audio_author.lower()

                                if new_match and not old_match:
                                    # Audio confirms new author - proceed with change
                                    logger.info(f"TRUST THE PROCESS: Audio confirms change to '{new_author}'")
                                    if audio_title:
                                        new_title = audio_title
                                    drastic_change = False  # Allow auto-fix
                                elif old_match and not new_match:
                                    # Audio says keep original - don't change
                                    logger.info(f"TRUST THE PROCESS: Audio says keep '{row['current_author']}'")
                                    new_author = row['current_author']
                                    drastic_change = False
                                else:
                                    # Audio is ambiguous - add to needs_attention list
                                    logger.warning(f"TRUST THE PROCESS: Audio ambiguous, flagging for attention")
                                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                      new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'needs_attention', ?, ?, ?, ?, ?, ?, ?)''',
                                             (row['book_id'], row['current_author'], row['current_title'],
                                              new_author, new_title, str(old_path), str(new_path),
                                              f"Unidentifiable: AI uncertain, audio ambiguous. Audio heard: {audio_author}",
                                              new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                              str(new_year) if new_year else None, new_edition, new_variant))
                                    c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                    processed += 1
                                    continue
                            else:
                                # No audio analysis possible - flag for attention
                                logger.warning(f"TRUST THE PROCESS: No audio available, flagging for attention")
                                c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                  new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                             VALUES (?, ?, ?, ?, ?, ?, ?, 'needs_attention', ?, ?, ?, ?, ?, ?, ?)''',
                                         (row['book_id'], row['current_author'], row['current_title'],
                                          new_author, new_title, str(old_path), str(new_path),
                                          f"Unidentifiable: AI uncertain, no audio analysis available",
                                          new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                          str(new_year) if new_year else None, new_edition, new_variant))
                                c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                processed += 1
                                continue
                        else:
                            # Standard mode - block the change
                            logger.warning(f"BLOCKED (uncertain): {row['current_author']} -> {new_author}")
                            # Record as pending_fix for manual review
                            c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                              new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                         VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix', ?, ?, ?, ?, ?, ?, ?)''',
                                     (row['book_id'], row['current_author'], row['current_title'],
                                      new_author, new_title, str(old_path), str(new_path),
                                      f"Uncertain: {verification.get('reasoning', 'needs review')}",
                                      new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                      str(new_year) if new_year else None, new_edition, new_variant))
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                            processed += 1
                            continue
                else:
                    # Verification failed completely - check Trust the Process mode
                    trust_mode = config.get('trust_the_process', False)
                    if trust_mode and config.get('gemini_api_key'):
                        # Try audio analysis as last resort
                        logger.info(f"TRUST THE PROCESS: Verification failed, trying audio as last resort...")
                        audio_files = find_audio_files(str(old_path))
                        if audio_files:
                            audio_result = analyze_audio_with_gemini(audio_files[0], config)
                            if audio_result and audio_result.get('author'):
                                # Use audio result directly
                                new_author = audio_result.get('author', new_author)
                                new_title = audio_result.get('title', new_title)
                                new_narrator = audio_result.get('narrator', new_narrator)
                                drastic_change = is_drastic_author_change(row['current_author'], new_author)
                                logger.info(f"TRUST THE PROCESS: Using audio metadata: {new_author} - {new_title}")
                            else:
                                # Audio failed too - flag for attention
                                c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                  new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                             VALUES (?, ?, ?, ?, ?, ?, ?, 'needs_attention', ?, ?, ?, ?, ?, ?, ?)''',
                                         (row['book_id'], row['current_author'], row['current_title'],
                                          new_author, new_title, str(old_path), str(new_path),
                                          f"Unidentifiable: All verification methods failed",
                                          new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                          str(new_year) if new_year else None, new_edition, new_variant))
                                c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                processed += 1
                                continue
                        else:
                            # No audio files - flag for attention
                            c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                              new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                         VALUES (?, ?, ?, ?, ?, ?, ?, 'needs_attention', ?, ?, ?, ?, ?, ?, ?)''',
                                     (row['book_id'], row['current_author'], row['current_title'],
                                      new_author, new_title, str(old_path), str(new_path),
                                      f"Unidentifiable: No verification data, no audio files",
                                      new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                      str(new_year) if new_year else None, new_edition, new_variant))
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                            processed += 1
                            continue
                    else:
                        # Standard mode - block the change
                        logger.warning(f"BLOCKED (verification failed): {row['current_author']} -> {new_author}")
                        c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        processed += 1
                        continue

                # Recalculate new_path with potentially updated author/title/narrator
                new_path = build_new_path(lib_path, new_author, new_title,
                                          series=new_series, series_num=new_series_num,
                                          narrator=new_narrator, year=new_year,
                                          edition=new_edition, variant=new_variant, config=config)

                # CRITICAL SAFETY: Check recalculated path
                if new_path is None:
                    logger.error(f"SAFETY BLOCK: Invalid recalculated path for '{new_author}' / '{new_title}'")
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('error', 'Path validation failed after verification', row['book_id']))
                    conn.commit()
                    processed += 1
                    continue

            # Only auto-fix if enabled AND NOT a drastic change (unless Trust the Process mode)
            # In Trust the Process mode, verified drastic changes can be auto-fixed
            trust_mode = config.get('trust_the_process', False)
            can_auto_fix = config.get('auto_fix', False) and (not drastic_change or trust_mode)
            if can_auto_fix:
                # Actually rename the folder
                try:
                    import shutil

                    if new_path.exists():
                        # Destination already exists - check if it has files
                        existing_files = list(new_path.iterdir())
                        if existing_files:
                            # Try to find a unique path by adding version distinguishers
                            logger.info(f"CONFLICT: {new_path} exists, trying version-aware naming...")
                            resolved_path = None

                            # Try distinguishers in order: narrator, variant, edition, year
                            # Only try if we have the data AND it's not already in the path
                            distinguishers_to_try = []

                            if new_narrator and new_narrator not in str(new_path):
                                distinguishers_to_try.append(('narrator', new_narrator, None, None))
                            if new_variant and new_variant not in str(new_path):
                                distinguishers_to_try.append(('variant', None, None, new_variant))
                            if new_edition and new_edition not in str(new_path):
                                distinguishers_to_try.append(('edition', None, new_edition, None))
                            if new_year and str(new_year) not in str(new_path):
                                distinguishers_to_try.append(('year', None, None, None))

                            for dist_type, narrator_val, edition_val, variant_val in distinguishers_to_try:
                                test_path = build_new_path(
                                    lib_path, new_author, new_title,
                                    series=new_series, series_num=new_series_num,
                                    narrator=narrator_val or new_narrator,
                                    year=new_year if dist_type == 'year' else None,
                                    edition=edition_val,
                                    variant=variant_val,
                                    config=config
                                )
                                if test_path and not test_path.exists():
                                    resolved_path = test_path
                                    logger.info(f"Resolved conflict using {dist_type}: {resolved_path}")
                                    break

                            if resolved_path:
                                new_path = resolved_path
                            else:
                                # Couldn't resolve with distinguishers - check if it's actually a duplicate
                                logger.info(f"Comparing folders to check for duplicate: {old_path} vs {new_path}")
                                comparison = compare_book_folders(old_path, new_path)

                                # Check for corrupt file scenarios first
                                if comparison.get('dest_corrupt'):
                                    # Destination is corrupt - source is valid, move source to version path
                                    # Don't replace corrupt dest - let user deal with that
                                    logger.warning(f"CORRUPT DEST: {new_path} has corrupt/unreadable files, source {old_path} is valid - moving source to version path")

                                    # Create version path for the valid source
                                    version_path = build_new_path(
                                        lib_path, new_author, new_title,
                                        series=new_series, series_num=new_series_num,
                                        narrator=new_narrator,
                                        variant="Valid Copy" if not new_variant else f"{new_variant}, Valid Copy",
                                        config=config
                                    )
                                    if version_path and not version_path.exists():
                                        new_path = version_path
                                        logger.info(f"Moving valid source to: {new_path}")
                                        # Fall through to the move code below
                                    else:
                                        # Can't create version path - just record the issue
                                        reason = comparison.get('reason', 'Destination files are corrupt/unreadable')
                                        c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                          new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                                     VALUES (?, ?, ?, ?, ?, ?, ?, 'corrupt_dest', ?, ?, ?, ?, ?, ?, ?)''',
                                                 (row['book_id'], row['current_author'], row['current_title'],
                                                  new_author, new_title, str(old_path), str(new_path),
                                                  f"{reason}. Source: {comparison['source_files']} files, Dest: {comparison['dest_files']} files (corrupt)",
                                                  new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                                  str(new_year) if new_year else None, new_edition, new_variant))
                                        c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                                 ('corrupt_dest', f'Destination {new_path} is corrupt - source is valid', row['book_id']))
                                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                        processed += 1
                                        continue

                                if comparison.get('source_corrupt'):
                                    # Source is corrupt - mark as duplicate (keep dest)
                                    logger.warning(f"CORRUPT SOURCE: {old_path} has corrupt/unreadable files, dest {new_path} is valid")
                                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                      new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'duplicate', ?, ?, ?, ?, ?, ?, ?)''',
                                             (row['book_id'], row['current_author'], row['current_title'],
                                              new_author, new_title, str(old_path), str(new_path),
                                              f"Source is corrupt/unreadable, destination is valid. Recommend removing corrupt source.",
                                              new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                              str(new_year) if new_year else None, new_edition, new_variant))
                                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                             ('duplicate', f'Corrupt - valid copy exists at {new_path}', row['book_id']))
                                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                    processed += 1
                                    continue

                                if comparison['identical'] or comparison['same_book']:
                                    # It's a duplicate! Mark for removal instead of conflict
                                    logger.info(f"DUPLICATE DETECTED: {old_path} is duplicate of {new_path} "
                                               f"(identical={comparison['identical']}, overlap={comparison['overlap_ratio']:.0%})")

                                    # Determine which to keep based on recommendation
                                    recommendation = comparison.get('recommendation', 'keep_dest')
                                    reason = comparison.get('reason', '')

                                    if recommendation == 'keep_source' and comparison['source_better']:
                                        # Source is better - note this for user review
                                        logger.info(f"Note: Source is better ({reason})")

                                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                      new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'duplicate', ?, ?, ?, ?, ?, ?, ?)''',
                                             (row['book_id'], row['current_author'], row['current_title'],
                                              new_author, new_title, str(old_path), str(new_path),
                                              f"Duplicate detected ({comparison['overlap_ratio']:.0%} match, {comparison['matching_count']} files). Source: {comparison['source_files']} files, Dest: {comparison['dest_files']} files. {reason}",
                                              new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                              str(new_year) if new_year else None, new_edition, new_variant))
                                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                             ('duplicate', f'Duplicate of {new_path}', row['book_id']))
                                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                    processed += 1
                                    continue
                                else:
                                    # Different versions - these are BOTH valid, create unique path for source
                                    # Don't error out - find a way to distinguish them and move!
                                    deep_info = comparison.get('deep_analysis', {})
                                    logger.info(f"DIFFERENT VERSIONS: {old_path.name} vs existing {new_path.name} - creating unique path")

                                    # Generate a distinguisher based on what we know about the source
                                    # Priority: narrator > file count > folder name hint
                                    version_distinguisher = None

                                    # Try to get narrator from source audio files
                                    if not new_narrator:
                                        try:
                                            source_narrator = extract_narrator_from_folder(old_path)
                                            if source_narrator:
                                                version_distinguisher = source_narrator
                                                logger.info(f"Using narrator from source: {source_narrator}")
                                        except Exception as e:
                                            logger.debug(f"Could not extract narrator: {e}")
                                    else:
                                        version_distinguisher = new_narrator

                                    # Fallback: use file characteristics
                                    if not version_distinguisher:
                                        src_files = comparison.get('source_files', 0)
                                        src_size_mb = comparison.get('source_size', 0) // 1024 // 1024
                                        # Create a distinguisher like "Version B" or use file count
                                        # Check what versions already exist
                                        existing_versions = []
                                        for sibling in new_path.parent.iterdir():
                                            if sibling.is_dir() and sibling.name.startswith(new_path.name):
                                                existing_versions.append(sibling.name)

                                        # Generate next version letter
                                        if not existing_versions:
                                            # Existing one is "A", new one is "B"
                                            version_distinguisher = "Version B"
                                        else:
                                            # Find next available letter
                                            used_letters = set()
                                            for v in existing_versions:
                                                if 'Version ' in v:
                                                    try:
                                                        letter = v.split('Version ')[1][0]
                                                        used_letters.add(letter)
                                                    except:
                                                        pass
                                            for letter in 'BCDEFGHIJKLMNOPQRSTUVWXYZ':
                                                if letter not in used_letters:
                                                    version_distinguisher = f"Version {letter}"
                                                    break

                                        logger.info(f"Using fallback distinguisher: {version_distinguisher}")

                                    # Build new path with distinguisher
                                    if version_distinguisher:
                                        # Add as variant (in brackets)
                                        unique_path = build_new_path(
                                            lib_path, new_author, new_title,
                                            series=new_series, series_num=new_series_num,
                                            narrator=new_narrator,
                                            variant=version_distinguisher if not new_variant else f"{new_variant}, {version_distinguisher}",
                                            config=config
                                        )
                                        if unique_path and not unique_path.exists():
                                            new_path = unique_path
                                            logger.info(f"Resolved to unique path: {new_path}")
                                            # Don't continue - fall through to the move code below
                                        else:
                                            # Still can't find unique path - now error
                                            logger.warning(f"CONFLICT: Could not create unique path for different version")
                                            c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                              new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                                         VALUES (?, ?, ?, ?, ?, ?, ?, 'error', ?, ?, ?, ?, ?, ?, ?)''',
                                                     (row['book_id'], row['current_author'], row['current_title'],
                                                      new_author, new_title, str(old_path), str(new_path),
                                                      f"Different version exists, could not generate unique path. Source: {comparison['source_files']} files, Dest: {comparison['dest_files']} files",
                                                      new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                                      str(new_year) if new_year else None, new_edition, new_variant))
                                            c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                                     ('conflict', 'Different version exists, unique path generation failed', row['book_id']))
                                            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                            processed += 1
                                            continue
                                    else:
                                        # No distinguisher at all - error
                                        logger.warning(f"CONFLICT: No distinguisher available for different version")
                                        c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                                          new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                                     VALUES (?, ?, ?, ?, ?, ?, ?, 'error', ?, ?, ?, ?, ?, ?, ?)''',
                                                 (row['book_id'], row['current_author'], row['current_title'],
                                                  new_author, new_title, str(old_path), str(new_path),
                                                  f"Different version exists, no distinguisher available. Source: {comparison['source_files']} files, Dest: {comparison['dest_files']} files",
                                                  new_narrator, new_series, str(new_series_num) if new_series_num else None,
                                                  str(new_year) if new_year else None, new_edition, new_variant))
                                        c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                                 ('conflict', 'Different version exists', row['book_id']))
                                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                        processed += 1
                                        continue
                        else:
                            # Destination is empty folder - safe to use it
                            shutil.move(str(old_path), str(new_path.parent / (new_path.name + "_temp")))
                            new_path.rmdir()
                            (new_path.parent / (new_path.name + "_temp")).rename(new_path)

                        # Clean up empty parent author folder
                        try:
                            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                                old_path.parent.rmdir()
                        except OSError:
                            pass  # Parent not empty, that's fine

                    if not new_path.exists():
                        # Destination doesn't exist - simple rename
                        new_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(old_path), str(new_path))

                        # Clean up empty parent author folder
                        try:
                            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                                old_path.parent.rmdir()
                        except OSError:
                            pass  # Parent not empty, that's fine

                    logger.info(f"Fixed: {row['current_author']}/{row['current_title']} -> {new_author}/{new_title}")

                    # Clean up any stale pending entries for this book before recording fix
                    c.execute("DELETE FROM history WHERE book_id = ? AND status = 'pending_fix'", (row['book_id'],))

                    # Record in history
                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status,
                                                      new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'fixed', ?, ?, ?, ?, ?, ?)''',
                             (row['book_id'], row['current_author'], row['current_title'],
                              new_author, new_title, str(old_path), str(new_path),
                              new_narrator, new_series, str(new_series_num) if new_series_num else None,
                              str(new_year) if new_year else None, new_edition, new_variant))
                    history_id = c.lastrowid  # Capture the newly inserted history record ID

                    # Update book record - handle case where another book already has this path
                    try:
                        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ?
                                     WHERE id = ?''',
                                 (str(new_path), new_author, new_title, 'fixed', row['book_id']))
                    except sqlite3.IntegrityError:
                        # Path already exists (duplicate book merged) - delete this book record
                        logger.info(f"Merged duplicate: {row['path']} -> existing {new_path}")
                        c.execute('DELETE FROM books WHERE id = ?', (row['book_id'],))

                    fixed += 1

                    # Embed metadata tags if enabled
                    if config.get('metadata_embedding_enabled', False):
                        try:
                            embed_metadata = build_metadata_for_embedding(
                                author=new_author,
                                title=new_title,
                                series=new_series,
                                series_num=str(new_series_num) if new_series_num else None,
                                narrator=new_narrator,
                                year=str(new_year) if new_year else None,
                                edition=new_edition,
                                variant=new_variant
                            )
                            embed_result = embed_tags_for_path(
                                new_path,
                                embed_metadata,
                                create_backup=config.get('metadata_embedding_backup_sidecar', True),
                                overwrite=config.get('metadata_embedding_overwrite_managed', True)
                            )
                            if embed_result['success']:
                                embed_status = 'ok'
                                embed_error = None
                                logger.info(f"Embedded tags in {embed_result['files_processed']} files at {new_path}")
                            else:
                                embed_status = 'error'
                                embed_error = embed_result.get('error') or '; '.join(embed_result.get('errors', []))[:500]
                                logger.warning(f"Tag embedding failed for {new_path}: {embed_error}")
                            # Update history with embed status using the captured history ID
                            c.execute('UPDATE history SET embed_status = ?, embed_error = ? WHERE id = ?',
                                     (embed_status, embed_error, history_id))
                        except Exception as embed_e:
                            logger.error(f"Tag embedding exception for {new_path}: {embed_e}")
                            c.execute('UPDATE history SET embed_status = ?, embed_error = ? WHERE id = ?',
                                     ('error', str(embed_e)[:500], history_id))

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error fixing {row['path']}: {error_msg}")
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('error', error_msg, row['book_id']))
            else:
                # Drastic change or auto_fix disabled - record as pending for manual review
                logger.info(f"PENDING APPROVAL: {row['current_author']} -> {new_author} (drastic={drastic_change})")
                c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status,
                                                  new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                             VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix', ?, ?, ?, ?, ?, ?)''',
                         (row['book_id'], row['current_author'], row['current_title'],
                          new_author, new_title, str(old_path), str(new_path),
                          new_narrator, new_series, str(new_series_num) if new_series_num else None,
                          str(new_year) if new_year else None, new_edition, new_variant))
                c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                fixed += 1
        else:
            # No fix needed
            c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
            logger.info(f"Verified OK: {row['current_author']}/{row['current_title']}")

        # Remove from queue
        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
        processed += 1

    # Update stats (INSERT if not exists first)
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('UPDATE stats SET fixed = COALESCE(fixed, 0) + ? WHERE date = ?', (fixed, today))

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 2/AI] Batch complete: {processed} processed, {fixed} fixed")
    return processed, fixed


def process_layer_3_audio(config, limit=None):
    """
    Layer 3: Audio Analysis

    Processes items at verification_layer=3 using Gemini audio analysis.
    This is the most expensive layer - extracts metadata from audiobook intros.
    Items that still can't be identified are marked as 'needs_attention'.
    """
    if not config.get('enable_audio_analysis', False):
        logger.info("[LAYER 3] Audio analysis disabled, skipping")
        return 0, 0

    # Check if we have Gemini API key
    secrets = load_secrets()
    if not secrets or not secrets.get('gemini_api_key'):
        logger.info("[LAYER 3] No Gemini API key for audio analysis, skipping")
        return 0, 0

    conn = get_db()
    c = conn.cursor()

    batch_size = limit or config.get('batch_size', 3)

    # Get items awaiting audio analysis (layer 3)
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.verification_layer = 3
                   AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (batch_size,))
    batch = c.fetchall()

    if not batch:
        conn.close()
        return 0, 0

    logger.info(f"[LAYER 3] Processing {len(batch)} items via audio analysis")

    processed = 0
    resolved = 0

    for row in batch:
        path = row['path']
        book_path = Path(path)

        # Find audio files in this folder
        audio_files = find_audio_files(str(book_path)) if book_path.is_dir() else [str(book_path)]

        if not audio_files:
            # No audio files - mark as needs attention
            logger.warning(f"[LAYER 3] No audio files found, marking needs attention: {path}")
            c.execute('UPDATE books SET status = ?, verification_layer = 4, error_message = ? WHERE id = ?',
                     ('needs_attention', 'No audio files found for analysis', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            processed += 1
            continue

        # Try audio analysis with Gemini
        audio_result = analyze_audio_with_gemini(audio_files[0], config)

        if audio_result and audio_result.get('author') and audio_result.get('title'):
            # Audio analysis succeeded - create pending fix with extracted metadata
            new_author = audio_result.get('author', '')
            new_title = audio_result.get('title', '')
            new_narrator = audio_result.get('narrator', '')
            new_series = audio_result.get('series', '')
            new_series_num = audio_result.get('series_num')

            logger.info(f"[LAYER 3] Audio extracted: {new_author}/{new_title} (narrator: {new_narrator})")

            current_author = row['current_author']
            current_title = row['current_title']

            # Check if extracted data differs from current values (returns 0.0-1.0)
            author_match = calculate_title_similarity(current_author, new_author) if current_author else 0
            title_match = calculate_title_similarity(current_title, new_title) if current_title else 0

            if author_match >= 0.90 and title_match >= 0.90:
                # Audio confirms current values are correct - mark verified
                logger.info(f"[LAYER 3] Audio confirms existing metadata, marking verified: {current_author}/{current_title}")
                c.execute('UPDATE books SET status = ?, verification_layer = 4 WHERE id = ?',
                         ('verified', row['book_id']))
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                resolved += 1
            else:
                # Audio suggests different values - create pending fix
                # Find library path
                lib_path = None
                for lp in config.get('library_paths', []):
                    lp_path = Path(lp)
                    try:
                        book_path.relative_to(lp_path)
                        lib_path = lp_path
                        break
                    except ValueError:
                        continue

                if lib_path is None:
                    lib_path = book_path.parent.parent
                    logger.warning(f"[LAYER 3] Book path {book_path} not under any configured library, guessing lib_path={lib_path}")

                new_path = build_new_path(lib_path, new_author, new_title,
                                          series=new_series, series_num=new_series_num,
                                          narrator=new_narrator, config=config)

                if new_path is None:
                    logger.error(f"[LAYER 3] SAFETY BLOCK: Invalid path for '{new_author}' / '{new_title}'")
                    c.execute('UPDATE books SET status = ?, verification_layer = 4, error_message = ? WHERE id = ?',
                             ('error', 'Audio extraction produced invalid path', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                else:
                    # Create pending fix for manual review
                    logger.info(f"[LAYER 3] Creating pending fix: {current_author}/{current_title} -> {new_author}/{new_title}")
                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                      new_narrator, new_series, new_series_num)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix', ?, ?, ?, ?)''',
                             (row['book_id'], current_author, current_title,
                              new_author, new_title, str(book_path), str(new_path),
                              'Identified via audio analysis',
                              new_narrator, new_series, str(new_series_num) if new_series_num else None))
                    c.execute('UPDATE books SET status = ?, verification_layer = 4 WHERE id = ?',
                             ('pending_fix', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    resolved += 1
        else:
            # Audio analysis failed - this is the last layer, mark as needs attention
            logger.warning(f"[LAYER 3] Audio analysis failed, marking needs attention: {path}")
            c.execute('UPDATE books SET status = ?, verification_layer = 4, error_message = ? WHERE id = ?',
                     ('needs_attention', 'All verification layers exhausted - manual review required', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))

        processed += 1

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 3] Processed {processed}, resolved {resolved} via audio")
    return processed, resolved


def apply_fix(history_id):
    """Apply a pending fix from history."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    fix = c.fetchone()

    if not fix:
        conn.close()
        return False, "Fix not found"

    old_path = Path(fix['old_path'])
    new_path = Path(fix['new_path'])

    # CRITICAL SAFETY: Validate paths before any file operations
    config = load_config()
    library_paths = [Path(p).resolve() for p in config.get('library_paths', [])]

    # Check old_path is in a library
    old_in_library = False
    for lib in library_paths:
        try:
            old_path.resolve().relative_to(lib)
            old_in_library = True
            break
        except ValueError:
            continue

    # Check new_path is in a library
    new_in_library = False
    for lib in library_paths:
        try:
            new_path.resolve().relative_to(lib)
            new_in_library = True
            break
        except ValueError:
            continue

    if not old_in_library or not new_in_library:
        error_msg = f"SAFETY BLOCK: Path outside library! old_in_lib={old_in_library}, new_in_lib={new_in_library}"
        logger.error(error_msg)
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('error', error_msg, history_id))
        conn.commit()
        conn.close()
        return False, error_msg

    # Check new_path has reasonable depth (at least 2 components: Author/Title)
    for lib in library_paths:
        try:
            relative = new_path.resolve().relative_to(lib)
            if len(relative.parts) < 2:
                error_msg = f"SAFETY BLOCK: Path too shallow ({len(relative.parts)} levels) - would dump at author level"
                logger.error(error_msg)
                c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                         ('error', error_msg, history_id))
                conn.commit()
                conn.close()
                return False, error_msg
            break
        except ValueError:
            continue

    if not old_path.exists():
        error_msg = f"Source no longer exists: {old_path}"
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('error', error_msg, history_id))
        conn.commit()
        conn.close()
        return False, error_msg

    try:
        import shutil

        # Check if we're moving a file (ebook/loose file/single m4b) vs a folder
        is_file_move = old_path.is_file()

        # If moving a single file, ensure new_path includes the filename with extension
        # (build_new_path returns a folder path, but for single files we need to include the filename)
        if is_file_move:
            # Check if new_path looks like a folder (no extension or doesn't match audio extension)
            audio_extensions = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus', '.wma', '.aac'}
            if new_path.suffix.lower() not in audio_extensions:
                # new_path is a folder, we need to create folder and put file inside
                file_dest = new_path / old_path.name
                logger.info(f"Single file move: {old_path.name} -> {file_dest}")
            else:
                file_dest = new_path

            if file_dest.exists():
                error_msg = f"Destination file already exists: {file_dest.name}"
                c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                         ('error', error_msg, history_id))
                conn.commit()
                conn.close()
                return False, error_msg

            # Create destination folder and move file
            file_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(file_dest))
            # Update new_path to the folder for embedding later
            new_path = file_dest.parent
        elif new_path.exists():
            # Moving a folder - check if destination has files
            existing_files = list(new_path.iterdir())
            if existing_files:
                # DON'T MERGE - this is likely a different narrator version
                error_msg = "Destination folder already exists with files - possible different narrator version"
                c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                         ('error', error_msg, history_id))
                conn.commit()
                conn.close()
                return False, error_msg
            else:
                # Destination is empty folder - safe to use it
                shutil.move(str(old_path), str(new_path.parent / (new_path.name + "_temp")))
                new_path.rmdir()
                (new_path.parent / (new_path.name + "_temp")).rename(new_path)
        else:
            # Destination doesn't exist - create parent folders and move
            new_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(new_path))

        # Clean up empty parent
        try:
            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                old_path.parent.rmdir()
        except OSError:
            pass

        # Update book record
        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ?
                     WHERE id = ?''',
                 (str(new_path), fix['new_author'], fix['new_title'], 'fixed', fix['book_id']))

        # Update history status
        c.execute('UPDATE history SET status = ? WHERE id = ?', ('fixed', history_id))

        # Embed metadata tags if enabled
        embed_status = None
        embed_error = None
        if config.get('metadata_embedding_enabled', False):
            try:
                embed_metadata = build_metadata_for_embedding(
                    author=fix['new_author'],
                    title=fix['new_title'],
                    series=fix['new_series'] if fix['new_series'] else None,
                    series_num=fix['new_series_num'] if fix['new_series_num'] else None,
                    narrator=fix['new_narrator'] if fix['new_narrator'] else None,
                    year=fix['new_year'] if fix['new_year'] else None,
                    edition=fix['new_edition'] if fix['new_edition'] else None,
                    variant=fix['new_variant'] if fix['new_variant'] else None
                )
                embed_result = embed_tags_for_path(
                    new_path,
                    embed_metadata,
                    create_backup=config.get('metadata_embedding_backup_sidecar', True),
                    overwrite=config.get('metadata_embedding_overwrite_managed', True)
                )
                if embed_result['success']:
                    embed_status = 'ok'
                    logger.info(f"Embedded tags in {embed_result['files_processed']} files at {new_path}")
                else:
                    embed_status = 'error'
                    embed_error = embed_result.get('error') or '; '.join(embed_result.get('errors', []))[:500]
                    logger.warning(f"Tag embedding failed for {new_path}: {embed_error}")
            except Exception as embed_e:
                embed_status = 'error'
                embed_error = str(embed_e)[:500]
                logger.error(f"Tag embedding exception for {new_path}: {embed_e}")

            # Update history with embed status
            c.execute('UPDATE history SET embed_status = ?, embed_error = ? WHERE id = ?',
                     (embed_status, embed_error, history_id))

        conn.commit()
        conn.close()
        return True, "Fix applied successfully"
    except Exception as e:
        error_msg = str(e)
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('error', error_msg, history_id))
        conn.commit()
        conn.close()
        return False, error_msg

# ============== BACKGROUND WORKER ==============

worker_thread = None
worker_running = False
processing_status = {"active": False, "processed": 0, "total": 0, "current": "", "errors": []}

def process_all_queue(config):
    """Process ALL items in the queue in batches, respecting rate limits.

    Uses layered processing:
    - Layer 1: API database lookups (fast, free)
    - Layer 2: AI verification (slower, rate-limited)
    - Layer 3: Audio analysis (slowest, expensive)
    """
    global processing_status

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as count FROM queue')
    total = c.fetchone()['count']
    conn.close()

    if total == 0:
        logger.info("Queue is empty, nothing to process")
        return 0, 0  # (total_processed, total_fixed)

    # Calculate delay between batches based on rate limit
    max_per_hour = config.get('max_requests_per_hour', 30)
    # Spread requests across the hour: 3600 seconds / max_requests
    min_delay = max(2, 3600 // max_per_hour)  # At least 2 seconds
    logger.info(f"Rate limit: {max_per_hour}/hour, delay between batches: {min_delay}s")

    processing_status = {"active": True, "processed": 0, "total": total, "current": "", "errors": [], "layer": 1}
    logger.info(f"=== STARTING LAYERED PROCESSING: {total} items in queue ===")

    # LAYER 1: API lookups (fast, no rate limit concerns)
    if config.get('enable_api_lookups', True):
        logger.info("=== LAYER 1: API Database Lookups ===")
        processing_status["layer"] = 1
        processing_status["current"] = "Layer 1: API lookups"
        layer1_processed = 0
        while True:
            processed, resolved = process_layer_1_api(config)
            if processed == 0:
                break
            layer1_processed += processed
            processing_status["processed"] = layer1_processed
            time.sleep(0.5)  # Small delay between batches
        logger.info(f"Layer 1 complete: {layer1_processed} items processed via API")

    # LAYER 2: AI Verification (rate-limited)
    logger.info("=== LAYER 2: AI Verification ===")
    processing_status["layer"] = 2
    processing_status["current"] = "Layer 2: AI verification"

    total_processed = 0
    total_fixed = 0
    batch_num = 0
    rate_limit_hits = 0

    while True:
        # Reload config each batch so settings changes take effect immediately
        config = load_config()

        # Check rate limit before processing
        allowed, calls_made, max_calls = check_rate_limit(config)
        if not allowed:
            rate_limit_hits += 1
            wait_time = min(300 * rate_limit_hits, 1800)  # 5 min, 10 min, 15 min... max 30 min
            logger.info(f"Rate limit reached ({calls_made}/{max_calls}), waiting {wait_time//60} minutes... (hit #{rate_limit_hits})")
            processing_status["current"] = f"Rate limited, waiting {wait_time//60}min... ({calls_made}/{max_calls})"
            time.sleep(wait_time)
            continue

        batch_num += 1
        logger.info(f"--- Processing batch {batch_num} (API: {calls_made}/{max_calls}) ---")

        processed, fixed = process_queue(config)

        if processed == 0:
            # Check if queue is actually empty or if there was an error
            conn = get_db()
            c = conn.cursor()
            c.execute('SELECT COUNT(*) as count FROM queue')
            remaining = c.fetchone()['count']
            conn.close()

            if remaining == 0:
                logger.info("Queue is now empty")
                break
            else:
                # Could be rate limit or API error
                logger.warning(f"No items processed but {remaining} remain")
                processing_status["errors"].append(f"Batch {batch_num}: No items processed, {remaining} remain")
                # Wait and retry once
                time.sleep(10)
                continue

        total_processed += processed
        total_fixed += fixed
        processing_status["processed"] = total_processed
        processing_status["current"] = f"Layer 2 Batch {batch_num}: {processed} processed"
        logger.info(f"Layer 2 Batch {batch_num} complete: {processed} processed, {fixed} fixed, {total_processed}/{total} total")

        # Rate limiting delay between batches
        logger.debug(f"Waiting {min_delay}s before next batch...")
        time.sleep(min_delay)

    # LAYER 3: Audio analysis (expensive, for items that passed through AI without resolution)
    if config.get('enable_audio_analysis', False):
        logger.info("=== LAYER 3: Audio Analysis ===")
        processing_status["layer"] = 3
        processing_status["current"] = "Layer 3: Audio analysis"
        layer3_processed = 0
        while True:
            processed, resolved = process_layer_3_audio(config)
            if processed == 0:
                break
            layer3_processed += processed
            total_processed += processed
            processing_status["processed"] = total_processed
            time.sleep(2)  # Longer delay for audio analysis (expensive)
        logger.info(f"Layer 3 complete: {layer3_processed} items processed via audio")

    processing_status["active"] = False
    processing_status["layer"] = 0
    logger.info(f"=== LAYERED PROCESSING COMPLETE: {total_processed} processed, {total_fixed} fixed ===")
    return total_processed, total_fixed

def background_worker():
    """Background worker that periodically scans and processes."""
    global worker_running

    logger.info("Background worker thread started")

    while worker_running:
        config = load_config()

        if config.get('enabled', True):
            try:
                logger.debug("Worker: Starting scan cycle")
                # Scan library
                scan_library(config)

                # Process queue if auto_fix is enabled
                if config.get('auto_fix', False):
                    logger.debug("Worker: Auto-fix enabled, processing queue")
                    process_all_queue(config)
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)

        # Sleep for scan interval
        interval = config.get('scan_interval_hours', 6) * 3600
        logger.debug(f"Worker: Sleeping for {interval} seconds")
        for _ in range(int(interval / 10)):
            if not worker_running:
                break
            time.sleep(10)

    logger.info("Background worker thread stopped")

def start_worker():
    """Start the background worker."""
    global worker_thread, worker_running

    if worker_thread and worker_thread.is_alive():
        logger.info("Worker already running")
        return

    worker_running = True
    worker_thread = threading.Thread(target=background_worker, daemon=True)
    worker_thread.start()
    logger.info("Background worker started")

def stop_worker():
    """Stop the background worker."""
    global worker_running
    worker_running = False
    logger.info("Background worker stop requested")

def is_worker_running():
    """Check if worker is actually running."""
    global worker_thread, worker_running
    return worker_running and worker_thread is not None and worker_thread.is_alive()

@app.context_processor
def inject_worker_status():
    """Inject worker_running into all templates automatically."""
    return {'worker_running': is_worker_running()}

# ============== ROUTES ==============

@app.route('/')
def dashboard():
    """Main dashboard."""
    conn = get_db()
    c = conn.cursor()

    # Get counts
    c.execute('SELECT COUNT(*) as count FROM books')
    total_books = c.fetchone()['count']

    c.execute('SELECT COUNT(*) as count FROM queue')
    queue_size = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'fixed'")
    fixed_count = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'verified'")
    verified_count = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    pending_fixes = c.fetchone()['count']

    # Get recent history (use LEFT JOIN in case book was deleted)
    c.execute('''SELECT h.*, b.path FROM history h
                 LEFT JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC LIMIT 15''')
    recent_history = c.fetchall()

    # Get stats for last 7 days
    c.execute('''SELECT date, scanned, queued, fixed, api_calls FROM stats
                 ORDER BY date DESC LIMIT 7''')
    daily_stats = c.fetchall()

    conn.close()

    config = load_config()

    return render_template('dashboard.html',
                          total_books=total_books,
                          queue_size=queue_size,
                          fixed_count=fixed_count,
                          verified_count=verified_count,
                          pending_fixes=pending_fixes,
                          recent_history=recent_history,
                          daily_stats=daily_stats,
                          config=config,
                          worker_running=worker_running)

@app.route('/orphans')
def orphans_page():
    """Redirect to unified Library view with orphan filter."""
    return redirect('/library?filter=orphan')

@app.route('/queue')
def queue_page():
    """Redirect to unified Library view with queue filter."""
    return redirect('/library?filter=queue')

@app.route('/history')
def history_page():
    """History of all fixes."""
    conn = get_db()
    c = conn.cursor()

    page = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status', None)
    per_page = 50
    offset = (page - 1) * per_page

    # Get duplicate count for the UI
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'duplicate'")
    duplicate_count = c.fetchone()['count']

    # Get needs_attention count for the UI
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'needs_attention'")
    needs_attention_count = c.fetchone()['count']

    # Get error count for the UI
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'error'")
    error_count = c.fetchone()['count']

    # Build query based on status filter
    if status_filter == 'pending':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     WHERE status = 'pending_fix'
                     ORDER BY fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    elif status_filter == 'duplicate':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'duplicate'")
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     WHERE status = 'duplicate'
                     ORDER BY fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    elif status_filter == 'attention':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'needs_attention'")
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     WHERE status = 'needs_attention'
                     ORDER BY fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    elif status_filter == 'error':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'error'")
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     WHERE status = 'error'
                     ORDER BY fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    else:
        c.execute('SELECT COUNT(*) as count FROM history')
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     ORDER BY fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    rows = c.fetchall()
    conn.close()

    # Convert to dicts and add is_drastic flag
    history_items = []
    for row in rows:
        item = dict(row)
        item['is_drastic'] = is_drastic_author_change(item.get('old_author'), item.get('new_author'))
        history_items.append(item)

    total_pages = (total + per_page - 1) // per_page

    return render_template('history.html',
                          history_items=history_items,
                          page=page,
                          total_pages=total_pages,
                          total=total,
                          status_filter=status_filter,
                          duplicate_count=duplicate_count,
                          needs_attention_count=needs_attention_count,
                          error_count=error_count)

@app.route('/settings', methods=['GET', 'POST'])
def settings_page():
    """Settings page."""
    if request.method == 'POST':
        # Load current config
        config = load_config()

        # Update config values
        config['library_paths'] = [p.strip() for p in request.form.get('library_paths', '').split('\n') if p.strip()]
        config['ai_provider'] = request.form.get('ai_provider', 'openrouter')
        config['openrouter_model'] = request.form.get('openrouter_model', 'google/gemma-3n-e4b-it:free')
        config['gemini_model'] = request.form.get('gemini_model', 'gemini-1.5-flash')
        config['ollama_url'] = request.form.get('ollama_url', 'http://localhost:11434').strip()
        config['ollama_model'] = request.form.get('ollama_model', 'llama3.2:3b').strip()
        config['scan_interval_hours'] = int(request.form.get('scan_interval_hours', 6))
        config['batch_size'] = int(request.form.get('batch_size', 3))
        config['max_requests_per_hour'] = int(request.form.get('max_requests_per_hour', 30))
        config['auto_fix'] = 'auto_fix' in request.form
        config['trust_the_process'] = 'trust_the_process' in request.form
        config['protect_author_changes'] = 'protect_author_changes' in request.form
        config['enabled'] = 'enabled' in request.form
        config['series_grouping'] = 'series_grouping' in request.form
        config['ebook_management'] = 'ebook_management' in request.form
        config['ebook_library_mode'] = request.form.get('ebook_library_mode', 'merge')
        # Verification layer settings (added beta.43)
        config['enable_api_lookups'] = 'enable_api_lookups' in request.form
        config['enable_ai_verification'] = 'enable_ai_verification' in request.form
        config['enable_audio_analysis'] = 'enable_audio_analysis' in request.form
        config['deep_scan_mode'] = 'deep_scan_mode' in request.form
        config['profile_confidence_threshold'] = int(request.form.get('profile_confidence_threshold', 85))
        config['skip_confirmations'] = 'skip_confirmations' in request.form
        config['anonymous_error_reporting'] = 'anonymous_error_reporting' in request.form
        config['error_reporting_include_titles'] = 'error_reporting_include_titles' in request.form
        config['metadata_embedding_enabled'] = 'metadata_embedding_enabled' in request.form
        config['metadata_embedding_overwrite_managed'] = 'metadata_embedding_overwrite_managed' in request.form
        config['metadata_embedding_backup_sidecar'] = 'metadata_embedding_backup_sidecar' in request.form
        # Language settings
        config['preferred_language'] = request.form.get('preferred_language', 'en')
        config['preserve_original_titles'] = 'preserve_original_titles' in request.form
        config['detect_language_from_audio'] = 'detect_language_from_audio' in request.form
        config['google_books_api_key'] = request.form.get('google_books_api_key', '').strip() or None
        config['update_channel'] = request.form.get('update_channel', 'stable')
        config['naming_format'] = request.form.get('naming_format', 'author/title')
        config['custom_naming_template'] = request.form.get('custom_naming_template', '{author}/{title}').strip()

        # Save config (without secrets)
        save_config(config)

        # Save secrets separately (preserving existing secrets like abs_api_token)
        secrets = load_secrets()
        secrets['openrouter_api_key'] = request.form.get('openrouter_api_key', '')
        secrets['gemini_api_key'] = request.form.get('gemini_api_key', '')
        save_secrets(secrets)

        return redirect(url_for('settings_page'))

    config = load_config()
    return render_template('settings.html', config=config, version=APP_VERSION)


# ============== PATH DIAGNOSTIC ==============

@app.route('/api/check_path', methods=['POST'])
def api_check_path():
    """
    Diagnostic endpoint to help users debug Docker volume mount issues.
    Shows what the container can actually see at a given path.
    """
    data = request.get_json() or {}
    path = data.get('path', '').strip()

    if not path:
        return jsonify({'success': False, 'error': 'No path provided'})

    result = {
        'path': path,
        'exists': os.path.exists(path),
        'is_directory': False,
        'is_file': False,
        'readable': False,
        'contents': [],
        'error': None,
        'suggestion': None
    }

    if not result['exists']:
        # Path doesn't exist - likely a Docker mount issue
        result['error'] = f"Path does not exist: {path}"
        result['suggestion'] = (
            "This path is not visible to the container. "
            "In Docker, you must mount host paths in your docker-compose.yml:\n\n"
            f"volumes:\n  - /your/host/path:{path}\n\n"
            "Then use the CONTAINER path (right side) in Settings, not the host path."
        )

        # Check if common mount points exist
        common_mounts = ['/audiobooks', '/data', '/media', '/books', '/library']
        existing_mounts = [m for m in common_mounts if os.path.exists(m)]
        if existing_mounts:
            result['available_mounts'] = existing_mounts
            result['suggestion'] += f"\n\nAvailable mount points: {', '.join(existing_mounts)}"

        return jsonify(result)

    result['is_directory'] = os.path.isdir(path)
    result['is_file'] = os.path.isfile(path)
    result['readable'] = os.access(path, os.R_OK)

    if not result['readable']:
        result['error'] = f"Path exists but is not readable (permission denied)"
        result['suggestion'] = "Check file permissions. The container user may not have access."
        return jsonify(result)

    if result['is_directory']:
        try:
            entries = sorted(os.listdir(path))[:50]  # Limit to first 50 entries
            result['contents'] = []
            result['total_entries'] = len(os.listdir(path))

            for entry in entries:
                entry_path = os.path.join(path, entry)
                entry_info = {
                    'name': entry,
                    'is_dir': os.path.isdir(entry_path),
                    'is_file': os.path.isfile(entry_path)
                }
                if entry_info['is_file']:
                    try:
                        entry_info['size'] = os.path.getsize(entry_path)
                    except:
                        entry_info['size'] = 0
                result['contents'].append(entry_info)

            if result['total_entries'] == 0:
                result['suggestion'] = "Directory exists but is empty. Are your audiobooks in a subdirectory?"
            else:
                result['success'] = True

        except Exception as e:
            result['error'] = f"Could not list directory: {e}"
    elif result['is_file']:
        result['error'] = "This is a file, not a directory. Library paths should be directories."
        result['suggestion'] = "Use the parent directory instead."

    result['success'] = result.get('success', result['exists'] and result['readable'] and result['is_directory'])
    return jsonify(result)


# ============== API ENDPOINTS ==============

@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Trigger a library scan."""
    config = load_config()
    checked, scanned, queued = scan_library(config)
    return jsonify({
        'success': True,
        'checked': checked,      # Total book folders examined
        'scanned': scanned,      # New books added to tracking
        'queued': queued         # Books needing fixes
    })

@app.route('/api/chaos_scan', methods=['POST'])
def api_chaos_scan():
    """
    Handle chaotic libraries with loose files dumped in root.
    Analyzes, groups, and identifies loose audio files.
    """
    config = load_config()
    library_paths = config.get('library_paths', [])

    if not library_paths:
        return jsonify({'success': False, 'error': 'No library paths configured'}), 400

    all_results = []
    for lib_path in library_paths:
        if os.path.exists(lib_path):
            results = handle_chaos_library(lib_path, config)
            all_results.extend(results)

    # Summarize
    identified = sum(1 for r in all_results if r.get('confidence') in ['high', 'medium'])
    needs_review = sum(1 for r in all_results if r.get('confidence') == 'low')
    failed = sum(1 for r in all_results if r.get('confidence') == 'none' or r.get('identification') == 'failed')

    return jsonify({
        'success': True,
        'groups': all_results,
        'summary': {
            'total_groups': len(all_results),
            'identified': identified,
            'needs_review': needs_review,
            'failed': failed
        }
    })

@app.route('/api/search_progress', methods=['GET'])
def api_search_progress():
    """
    Get current progress of ongoing search/scan operations.
    Returns queue position, percent complete, and current item being processed.
    """
    state = search_progress.get_state()
    return jsonify({
        'success': True,
        'active': state['active'],
        'operation': state['operation'],
        'percent': state['percent'],
        'processed': state['processed'],
        'total': state['total'],
        'current_item': state['current_item'],
        'queue_position': state['queue_position'],
        'queue': state['queue'][:10]  # Show next 10 in queue
    })

@app.route('/api/chaos_apply', methods=['POST'])
def api_chaos_apply():
    """
    Apply chaos handler results - create folders and move files.
    Expects JSON with groups to apply.
    """
    import shutil

    data = request.get_json()
    if not data or 'groups' not in data:
        return jsonify({'success': False, 'error': 'No groups provided'}), 400

    config = load_config()
    library_paths = config.get('library_paths', [])
    if not library_paths:
        return jsonify({'success': False, 'error': 'No library paths configured'}), 400

    lib_root = Path(library_paths[0])
    applied = 0
    errors = []
    skipped = []  # Items that couldn't be identified - left alone on disk

    for group in data['groups']:
        author = group.get('author', 'Unknown Author')
        title = group.get('title')
        files = group.get('files', [])
        confidence = group.get('confidence', 'none')
        identification = group.get('identification', '')

        if not title or not files:
            continue

        # Skip unidentified items - leave them alone on disk
        # Don't move files to "Unknown Author" folder - that's not helpful
        if author in ('Unknown Author', 'Unknown', None, '') or confidence == 'none' or identification == 'failed':
            skipped.append({
                'title': title or f"Unknown ({len(files)} files)",
                'files': files,
                'reason': 'Could not identify - left in place for manual review'
            })
            logger.info(f"CHAOS: Skipping unidentified group '{title}' - left in place")
            continue

        # Sanitize path components
        safe_author = sanitize_path_component(author)
        safe_title = sanitize_path_component(title)
        if not safe_author or not safe_title:
            errors.append(f"Invalid author/title: {author} / {title}")
            continue

        # Create target folder
        target_folder = lib_root / safe_author / safe_title

        try:
            target_folder.mkdir(parents=True, exist_ok=True)

            # Move files
            for file_path in files:
                src = Path(file_path)
                if src.exists() and src.is_file():
                    dst = target_folder / src.name
                    shutil.move(str(src), str(dst))
                    logger.info(f"CHAOS: Moved {src.name} -> {target_folder}")

            applied += 1

        except Exception as e:
            errors.append(f"Error with {title}: {str(e)}")
            logger.error(f"CHAOS APPLY ERROR: {e}")

    return jsonify({
        'success': True,
        'applied': applied,
        'skipped': skipped,  # Items left in place (couldn't identify)
        'skipped_count': len(skipped),
        'errors': errors
    })

@app.route('/api/deep_rescan', methods=['POST'])
def api_deep_rescan():
    """Deep re-scan: Reset all books and re-queue for fresh metadata lookup."""
    conn = get_db()
    c = conn.cursor()

    # Clear queue first
    c.execute('DELETE FROM queue')

    # Reset book statuses to force re-checking, BUT skip 'protected' books (user undid these)
    c.execute("UPDATE books SET status = 'pending' WHERE status != 'protected'")

    # Get count of protected books
    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'protected'")
    protected_count = c.fetchone()['count']

    # Get all non-protected books and add to queue
    c.execute("SELECT id, path FROM books WHERE status != 'protected'")
    books = c.fetchall()

    queued = 0
    for book in books:
        # Add to queue for re-processing
        c.execute('INSERT INTO queue (book_id, added_at) VALUES (?, ?)',
                  (book['id'], datetime.now().isoformat()))
        queued += 1

    conn.commit()
    conn.close()

    msg = f'Queued {queued} books for fresh metadata verification'
    if protected_count > 0:
        msg += f' ({protected_count} protected books skipped)'

    logger.info(f"Deep re-scan: {msg}")
    return jsonify({
        'success': True,
        'queued': queued,
        'protected': protected_count,
        'message': msg
    })


@app.route('/api/health_scan', methods=['POST'])
def api_health_scan():
    """
    Scan library for corrupt/incomplete audio files.
    Uses ffprobe to verify each file can be read properly.
    Returns list of problematic files grouped by book folder.
    """
    config = load_config()
    library_paths = config.get('library_paths', [])

    if not library_paths:
        return jsonify({'success': False, 'error': 'No library paths configured'})

    corrupt_files = []
    total_checked = 0
    total_duration = 0

    for lib_path in library_paths:
        lib_path = Path(lib_path)
        if not lib_path.exists():
            continue

        # Find all audio files
        audio_files = find_audio_files(str(lib_path))
        logger.info(f"Health scan: checking {len(audio_files)} audio files in {lib_path}")

        for audio_file in audio_files:
            total_checked += 1
            health = check_audio_file_health(audio_file)

            if not health['valid']:
                # Get folder info
                file_path = Path(audio_file)
                folder = file_path.parent
                size_mb = file_path.stat().st_size / (1024 * 1024) if file_path.exists() else 0

                corrupt_files.append({
                    'file': str(audio_file),
                    'folder': str(folder),
                    'folder_name': folder.name,
                    'filename': file_path.name,
                    'size_mb': round(size_mb, 1),
                    'error': health['error']
                })
            elif health['duration']:
                total_duration += health['duration']

    # Group by folder for easier review
    folders_with_issues = {}
    for cf in corrupt_files:
        folder = cf['folder']
        if folder not in folders_with_issues:
            folders_with_issues[folder] = {
                'folder': folder,
                'folder_name': cf['folder_name'],
                'files': [],
                'total_size_mb': 0
            }
        folders_with_issues[folder]['files'].append({
            'filename': cf['filename'],
            'size_mb': cf['size_mb'],
            'error': cf['error']
        })
        folders_with_issues[folder]['total_size_mb'] += cf['size_mb']

    result = {
        'success': True,
        'total_checked': total_checked,
        'total_healthy_duration_hours': round(total_duration / 3600, 1),
        'corrupt_count': len(corrupt_files),
        'folders_with_issues': list(folders_with_issues.values())
    }

    logger.info(f"Health scan complete: {total_checked} files checked, {len(corrupt_files)} corrupt")
    return jsonify(result)


@app.route('/api/delete_corrupt', methods=['POST'])
def api_delete_corrupt():
    """
    Delete a corrupt folder or file.
    Requires 'path' in request body - must be within a library path.
    """
    config = load_config()
    library_paths = config.get('library_paths', [])
    data = request.json if request.is_json else {}

    target_path = data.get('path')
    if not target_path:
        return jsonify({'success': False, 'error': 'No path provided'})

    target = Path(target_path)

    # Security: verify path is within a library path
    is_safe = False
    for lib_path in library_paths:
        try:
            target.relative_to(lib_path)
            is_safe = True
            break
        except ValueError:
            continue

    if not is_safe:
        return jsonify({'success': False, 'error': 'Path not within library'})

    if not target.exists():
        return jsonify({'success': False, 'error': 'Path does not exist'})

    try:
        import shutil
        if target.is_dir():
            shutil.rmtree(target)
            logger.info(f"Deleted corrupt folder: {target}")
        else:
            target.unlink()
            logger.info(f"Deleted corrupt file: {target}")

        return jsonify({'success': True, 'message': f'Deleted: {target}'})
    except Exception as e:
        logger.error(f"Failed to delete {target}: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/process', methods=['POST'])
def api_process():
    """Process the queue."""
    config = load_config()
    data = request.json if request.is_json else {}
    process_all = data.get('all', False)
    limit = data.get('limit')

    logger.info(f"API process called: all={process_all}, limit={limit}")

    if process_all:
        # Process entire queue in batches
        processed, fixed = process_all_queue(config)
    else:
        processed, fixed = process_queue(config, limit)

    return jsonify({'success': True, 'processed': processed, 'fixed': fixed})

@app.route('/api/process_status')
def api_process_status():
    """Get current processing status."""
    return jsonify(processing_status)

@app.route('/api/apply_fix/<int:history_id>', methods=['POST'])
def api_apply_fix(history_id):
    """Apply a specific fix."""
    success, message = apply_fix(history_id)
    return jsonify({'success': success, 'message': message})

@app.route('/api/reject_fix/<int:history_id>', methods=['POST'])
def api_reject_fix(history_id):
    """Reject a pending fix - delete it and mark book as OK."""
    conn = get_db()
    c = conn.cursor()

    # Get the history entry
    c.execute('SELECT book_id FROM history WHERE id = ?', (history_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'success': False, 'error': 'Fix not found'})

    book_id = row['book_id']

    # Delete the history entry
    c.execute('DELETE FROM history WHERE id = ?', (history_id,))

    # Mark book as verified/OK so it doesn't get re-queued
    c.execute("UPDATE books SET status = 'verified' WHERE id = ?", (book_id,))

    conn.commit()
    conn.close()

    logger.info(f"Rejected fix {history_id}, book {book_id} marked as verified")
    return jsonify({'success': True})

@app.route('/api/dismiss_error/<int:history_id>', methods=['POST'])
def api_dismiss_error(history_id):
    """Dismiss an error entry - just delete it from history."""
    conn = get_db()
    c = conn.cursor()

    # Get the history entry
    c.execute('SELECT book_id, status FROM history WHERE id = ?', (history_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'success': False, 'error': 'Entry not found'})

    # Delete the history entry
    c.execute('DELETE FROM history WHERE id = ?', (history_id,))

    # If the book still exists, mark it as verified so it doesn't keep erroring
    if row['book_id']:
        c.execute("UPDATE books SET status = 'verified' WHERE id = ?", (row['book_id'],))

    conn.commit()
    conn.close()

    logger.info(f"Dismissed error entry {history_id}")
    return jsonify({'success': True})

@app.route('/api/apply_all_pending', methods=['POST'])
def api_apply_all_pending():
    """Apply all pending fixes."""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM history WHERE status = 'pending_fix'")
    pending = c.fetchall()
    conn.close()

    applied = 0
    errors = 0
    for row in pending:
        success, _ = apply_fix(row['id'])
        if success:
            applied += 1
        else:
            errors += 1

    return jsonify({
        'success': True,
        'applied': applied,
        'errors': errors,
        'message': f'Applied {applied} fixes, {errors} errors'
    })


@app.route('/api/reject_all_pending', methods=['POST'])
def api_reject_all_pending():
    """Reject all pending fixes, resetting books to pending status."""
    conn = get_db()
    c = conn.cursor()

    # Get count first
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    count = c.fetchone()['count']

    if count == 0:
        conn.close()
        return jsonify({'success': True, 'message': 'No pending fixes to reject', 'rejected': 0})

    # Reset books to pending status (NOT verified - user just rejected the proposed fix)
    # Reset verification_layer so they can be re-processed if desired
    c.execute('''UPDATE books SET status = 'pending', verification_layer = 0
                 WHERE id IN (SELECT book_id FROM history WHERE status = 'pending_fix')''')

    # Delete all pending fix entries
    c.execute("DELETE FROM history WHERE status = 'pending_fix'")
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Rejected {count} pending fixes (books reset to pending)',
        'rejected': count
    })


@app.route('/api/remove_from_queue/<int:queue_id>', methods=['POST'])
def api_remove_from_queue(queue_id):
    """Remove an item from the queue."""
    conn = get_db()
    c = conn.cursor()

    # Get book_id first
    c.execute('SELECT book_id FROM queue WHERE id = ?', (queue_id,))
    row = c.fetchone()
    if row:
        c.execute('DELETE FROM queue WHERE id = ?', (queue_id,))
        c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
        conn.commit()

    conn.close()
    return jsonify({'success': True})


@app.route('/api/clear_queue', methods=['POST'])
def api_clear_queue():
    """Clear all items from the queue, marking them as verified."""
    conn = get_db()
    c = conn.cursor()

    # Get count first
    c.execute('SELECT COUNT(*) as count FROM queue')
    count = c.fetchone()['count']

    if count == 0:
        conn.close()
        return jsonify({'success': True, 'message': 'Queue already empty', 'cleared': 0})

    # Reset queued books to pending status (NOT verified - they weren't actually verified!)
    # Also reset verification_layer so they can be re-processed if re-scanned
    c.execute('''UPDATE books SET status = 'pending', verification_layer = 0
                 WHERE id IN (SELECT book_id FROM queue)''')

    # Clear the queue
    c.execute('DELETE FROM queue')
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Cleared {count} items from queue (reset to pending)',
        'cleared': count
    })


@app.route('/api/find_drastic_changes')
def api_find_drastic_changes():
    """Find history items where author changed drastically - potential mistakes."""
    conn = get_db()
    c = conn.cursor()

    # Get all fixed items where old and new path differ
    c.execute('''SELECT * FROM history
                 WHERE status = 'fixed' AND old_path != new_path
                 ORDER BY fixed_at DESC''')
    items = c.fetchall()
    conn.close()

    drastic_items = []
    for item in items:
        if is_drastic_author_change(item['old_author'], item['new_author']):
            drastic_items.append({
                'id': item['id'],
                'old_author': item['old_author'],
                'old_title': item['old_title'],
                'new_author': item['new_author'],
                'new_title': item['new_title'],
                'fixed_at': item['fixed_at']
            })

    return jsonify({
        'count': len(drastic_items),
        'items': drastic_items[:50]  # Limit to 50 for UI
    })

@app.route('/api/undo_all_drastic', methods=['POST'])
def api_undo_all_drastic():
    """Undo all drastic author changes."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get all fixed items
    c.execute('''SELECT * FROM history
                 WHERE status = 'fixed' AND old_path != new_path''')
    items = c.fetchall()

    undone = 0
    errors = 0

    for item in items:
        if not is_drastic_author_change(item['old_author'], item['new_author']):
            continue

        old_path = item['old_path']
        new_path = item['new_path']

        # Check if paths exist correctly
        if not os.path.exists(new_path):
            continue  # Already moved or doesn't exist
        if os.path.exists(old_path):
            continue  # Original location already exists

        try:
            shutil.move(new_path, old_path)
            c.execute('''UPDATE history SET status = 'undone', error_message = 'Auto-undone: drastic author change'
                         WHERE id = ?''', (item['id'],))
            c.execute('''UPDATE books SET
                         current_author = ?, current_title = ?, path = ?, status = 'protected'
                         WHERE id = ?''',
                      (item['old_author'], item['old_title'], old_path, item['book_id']))
            undone += 1
            logger.info(f"Auto-undone drastic change: {item['new_author']} -> {item['old_author']}")
        except Exception as e:
            errors += 1
            logger.error(f"Failed to undo {item['id']}: {e}")

    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'undone': undone,
        'errors': errors,
        'message': f'Undone {undone} drastic changes, {errors} errors'
    })

@app.route('/api/undo/<int:history_id>', methods=['POST'])
def api_undo(history_id):
    """Undo a fix - rename folder back to original name and restore original tags."""
    import shutil
    from audio_tagging import restore_tags_from_sidecar

    conn = get_db()
    c = conn.cursor()

    # Get the history record
    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    record = c.fetchone()

    if not record:
        conn.close()
        return jsonify({'success': False, 'error': 'History record not found'}), 404

    old_path = record['old_path']
    new_path = record['new_path']

    # Check if the new_path exists (current location)
    if not os.path.exists(new_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Current path not found: {new_path}'
        }), 404

    # Check if old_path already exists (would cause conflict)
    if os.path.exists(old_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Original path already exists: {old_path}'
        }), 409

    try:
        new_path_obj = Path(new_path)
        
        # Determine folder for sidecar (if new_path is a file, parent has sidecar)
        if new_path_obj.is_file():
            sidecar_folder = new_path_obj.parent
        else:
            sidecar_folder = new_path_obj

        # Restore original tags from sidecar backup before moving
        tags_restored = False
        tags_message = ""
        try:
            restore_result = restore_tags_from_sidecar(
                sidecar_folder,
                delete_sidecar_on_success=True  # Clean up sidecar after successful restore
            )
            if restore_result['files_restored'] > 0:
                tags_restored = True
                tags_message = f" Tags restored for {restore_result['files_restored']} file(s)."
                logger.info(f"Undo: Restored tags for {restore_result['files_restored']} files")
            elif restore_result.get('error'):
                tags_message = f" (Tag restore note: {restore_result['error']})"
        except Exception as tag_err:
            logger.warning(f"Could not restore tags during undo: {tag_err}")
            tags_message = " (Tags could not be restored)"

        # Rename back to original
        shutil.move(new_path, old_path)
        logger.info(f"Undo: Renamed '{new_path}' back to '{old_path}'")

        # Clean up empty parent folder if we moved a file
        if new_path_obj.is_file():
            try:
                parent = new_path_obj.parent
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    logger.info(f"Undo: Removed empty folder {parent}")
            except OSError:
                pass

        # Update history record
        c.execute('''UPDATE history SET status = 'undone', error_message = ?
                     WHERE id = ?''', (f'Manually undone by user{tags_message}', history_id))

        # Update book record back to original - use 'protected' status so deep rescan won't re-queue
        c.execute('''UPDATE books SET
                     current_author = ?, current_title = ?, path = ?, status = 'protected'
                     WHERE id = ?''',
                  (record['old_author'], record['old_title'], old_path, record['book_id']))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f"Undone! Renamed back to: {record['old_author']} / {record['old_title']}{tags_message}",
            'tags_restored': tags_restored
        })

    except Exception as e:
        conn.close()
        logger.error(f"Undo failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/remove_duplicate/<int:history_id>', methods=['POST'])
def api_remove_duplicate(history_id):
    """Remove a confirmed duplicate - deletes the source folder (old_path) since destination is properly named."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get the history record
    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    record = c.fetchone()

    if not record:
        conn.close()
        return jsonify({'success': False, 'error': 'History record not found'}), 404

    if record['status'] != 'duplicate':
        conn.close()
        return jsonify({'success': False, 'error': 'This record is not marked as a duplicate'}), 400

    old_path = record['old_path']  # This is the duplicate to remove
    new_path = record['new_path']  # This is the properly named copy to keep

    # Safety checks
    if not os.path.exists(old_path):
        # Already removed - just update status
        c.execute('UPDATE history SET status = ? WHERE id = ?', ('duplicate_removed', history_id))
        c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Duplicate was already removed'})

    if not os.path.exists(new_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'The properly named copy no longer exists: {new_path}. Refusing to delete.'
        }), 400

    # Re-verify it's still a duplicate before deleting
    comparison = compare_book_folders(old_path, new_path)
    if not (comparison['identical'] or comparison['same_book']):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Files have changed - no longer appears to be a duplicate (overlap: {comparison["overlap_ratio"]:.0%}). Please review manually.'
        }), 400

    try:
        # Get size before removal for logging
        total_size = sum(f.stat().st_size for f in Path(old_path).rglob('*') if f.is_file())

        # Remove the duplicate folder
        shutil.rmtree(old_path)
        logger.info(f"Removed duplicate: {old_path} ({total_size // 1024 // 1024}MB)")

        # Clean up empty parent folder (e.g., Unknown/)
        parent = Path(old_path).parent
        try:
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
                logger.info(f"Removed empty parent folder: {parent}")
        except OSError:
            pass

        # Update records
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('duplicate_removed', f'Removed {total_size // 1024 // 1024}MB duplicate', history_id))
        c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Removed duplicate ({total_size // 1024 // 1024}MB freed)',
            'removed_path': old_path,
            'kept_path': new_path
        })

    except Exception as e:
        conn.close()
        logger.error(f"Failed to remove duplicate: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/replace_corrupt/<int:history_id>', methods=['POST'])
def api_replace_corrupt(history_id):
    """Replace corrupt destination with valid source - delete corrupt, move source to dest path."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get the history record
    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    record = c.fetchone()

    if not record:
        conn.close()
        return jsonify({'success': False, 'error': 'History record not found'}), 404

    if record['status'] != 'corrupt_dest':
        conn.close()
        return jsonify({'success': False, 'error': 'This record is not marked as corrupt_dest'}), 400

    old_path = record['old_path']  # Valid source
    new_path = record['new_path']  # Corrupt destination

    # Safety checks
    if not os.path.exists(old_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Valid source no longer exists: {old_path}'
        }), 400

    try:
        # Get sizes for logging
        source_size = sum(f.stat().st_size for f in Path(old_path).rglob('*') if f.is_file())

        # Delete the corrupt destination if it exists
        if os.path.exists(new_path):
            dest_size = sum(f.stat().st_size for f in Path(new_path).rglob('*') if f.is_file())
            shutil.rmtree(new_path)
            logger.info(f"Deleted corrupt destination: {new_path} ({dest_size // 1024 // 1024}MB)")

        # Create parent directories if needed
        Path(new_path).parent.mkdir(parents=True, exist_ok=True)

        # Move the valid source to the destination
        shutil.move(old_path, new_path)
        logger.info(f"Moved valid source to destination: {old_path} -> {new_path}")

        # Clean up empty parent folder
        parent = Path(old_path).parent
        try:
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
                logger.info(f"Removed empty parent folder: {parent}")
        except OSError:
            pass

        # Update records
        c.execute('''UPDATE history SET status = ?, error_message = ? WHERE id = ?''',
                 ('fixed', f'Replaced corrupt destination with valid source ({source_size // 1024 // 1024}MB)', history_id))
        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ? WHERE id = ?''',
                 (new_path, record['new_author'], record['new_title'], 'fixed', record['book_id']))
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Replaced corrupt destination with valid source ({source_size // 1024 // 1024}MB)',
            'new_path': new_path
        })

    except Exception as e:
        conn.close()
        logger.error(f"Failed to replace corrupt destination: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/remove_all_duplicates', methods=['POST'])
def api_remove_all_duplicates():
    """Remove all confirmed duplicates in one operation."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get all duplicate records
    c.execute("SELECT * FROM history WHERE status = 'duplicate'")
    duplicates = c.fetchall()

    if not duplicates:
        conn.close()
        return jsonify({'success': True, 'message': 'No duplicates to remove', 'removed': 0})

    removed = 0
    skipped = 0
    errors = []
    total_freed = 0

    for record in duplicates:
        old_path = record['old_path']
        new_path = record['new_path']

        # Skip if already removed
        if not os.path.exists(old_path):
            c.execute('UPDATE history SET status = ? WHERE id = ?', ('duplicate_removed', record['id']))
            c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
            removed += 1
            continue

        # Skip if destination no longer exists
        if not os.path.exists(new_path):
            skipped += 1
            errors.append(f"Skipped {old_path}: destination {new_path} no longer exists")
            continue

        # Re-verify
        comparison = compare_book_folders(old_path, new_path)
        if not (comparison['identical'] or comparison['same_book']):
            skipped += 1
            errors.append(f"Skipped {old_path}: no longer appears to be duplicate")
            continue

        try:
            total_size = sum(f.stat().st_size for f in Path(old_path).rglob('*') if f.is_file())
            shutil.rmtree(old_path)
            total_freed += total_size

            # Clean up empty parent
            parent = Path(old_path).parent
            try:
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
            except OSError:
                pass

            c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                     ('duplicate_removed', f'Removed {total_size // 1024 // 1024}MB duplicate', record['id']))
            c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
            removed += 1

        except Exception as e:
            skipped += 1
            errors.append(f"Failed to remove {old_path}: {e}")

    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'removed': removed,
        'skipped': skipped,
        'freed_mb': total_freed // 1024 // 1024,
        'errors': errors[:10] if errors else []  # Limit errors in response
    })


@app.route('/api/clear_all_errors', methods=['POST'])
def api_clear_all_errors():
    """Clear all error entries from history."""
    conn = get_db()
    c = conn.cursor()

    # Get count before clearing
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'error'")
    count = c.fetchone()['count']

    if count == 0:
        conn.close()
        return jsonify({'success': True, 'message': 'No errors to clear', 'cleared': 0})

    # Delete all error entries
    c.execute("DELETE FROM history WHERE status = 'error'")
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Cleared {count} error entries',
        'cleared': count
    })


@app.route('/api/stats')
def api_stats():
    """Get current stats."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT COUNT(*) as count FROM books')
    total = c.fetchone()['count']

    c.execute('SELECT COUNT(*) as count FROM queue')
    queue = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'fixed'")
    fixed = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    pending = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'verified'")
    verified = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'structure_reversed'")
    structure_reversed = c.fetchone()['count']

    conn.close()

    return jsonify({
        'total_books': total,
        'queue_size': queue,
        'fixed': fixed,
        'pending_fixes': pending,
        'verified': verified,
        'structure_reversed': structure_reversed,
        'worker_running': is_worker_running(),
        'processing': processing_status
    })

@app.route('/api/queue')
def api_queue():
    """Get current queue items as JSON."""
    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT q.id, q.reason, q.added_at,
                        b.id as book_id, b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 ORDER BY q.priority, q.added_at''')
    items = [dict(row) for row in c.fetchall()]

    conn.close()
    return jsonify({'items': items, 'count': len(items)})


@app.route('/api/error_reports')
def api_error_reports():
    """Get stored anonymous error reports (for debug menu)."""
    reports = get_error_reports()
    return jsonify({
        'success': True,
        'reports': reports,
        'count': len(reports)
    })


@app.route('/api/error_reports/clear', methods=['POST'])
def api_clear_error_reports():
    """Clear all stored error reports."""
    clear_error_reports()
    return jsonify({'success': True, 'message': 'Error reports cleared'})


@app.route('/api/error_reports/send', methods=['POST'])
def api_send_error_reports():
    """Send error reports to the developer via email."""
    reports = get_error_reports()

    if not reports:
        return jsonify({
            'success': False,
            'error': 'No error reports to send'
        })

    # Get optional user message from request
    data = request.get_json() or {}
    user_message = data.get('message', '').strip()

    result = send_error_report_email(reports, user_message if user_message else None)
    return jsonify(result)


@app.route('/api/analyze_path', methods=['POST'])
def api_analyze_path():
    """
    Analyze a path to understand its structure (Author/Series/Book).
    Uses smart analysis: script first, Gemini AI for ambiguous cases.

    POST body: {"path": "/path/to/folder"}
    """
    data = request.get_json() or {}
    path = data.get('path')

    if not path:
        return jsonify({'error': 'path is required'}), 400

    config = load_config()
    lib_paths = config.get('library_paths', [])

    # Find which library this path belongs to
    library_root = None
    for lib in lib_paths:
        if path.startswith(lib):
            library_root = lib
            break

    if not library_root:
        # Try parent folders
        p = Path(path)
        for lib in lib_paths:
            if str(p).startswith(lib) or str(p.parent).startswith(lib):
                library_root = lib
                break

    if not library_root and lib_paths:
        library_root = lib_paths[0]  # Default to first library

    if not library_root:
        return jsonify({'error': 'No library paths configured'}), 400

    result = smart_analyze_path(path, library_root, config)
    return jsonify(result)


@app.route('/api/structure_reversed')
def api_structure_reversed():
    """Get items with reversed folder structure (Series/Author instead of Author/Series)."""
    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT id, path, current_author, current_title
                 FROM books
                 WHERE status = 'structure_reversed'
                 ORDER BY path''')
    items = []
    for row in c.fetchall():
        items.append({
            'id': row['id'],
            'path': row['path'],
            'detected_series': row['current_author'],  # What we think is the series/title
            'detected_author': row['current_title'],   # What we think is the author
            'suggestion': f"Move to: {row['current_title']}/{row['current_author']}"
        })

    conn.close()
    return jsonify({'items': items, 'count': len(items)})


@app.route('/api/structure_reversed/fix/<int:book_id>', methods=['POST'])
def api_fix_structure_reversed(book_id):
    """Fix a reversed structure by swapping author/title in the path."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT * FROM books WHERE id = ?', (book_id,))
    book = c.fetchone()
    if not book:
        return jsonify({'success': False, 'error': 'Book not found'}), 404

    if book['status'] != 'structure_reversed':
        return jsonify({'success': False, 'error': 'Book is not marked as structure_reversed'}), 400

    old_path = Path(book['path'])
    detected_series = book['current_author']  # This is actually the series/title
    detected_author = book['current_title']   # This is actually the author

    # Build new path: Author/Series (or Author/Title if no series)
    lib_root = old_path.parent.parent  # Go up from Title/Author to library root
    new_path = lib_root / detected_author / detected_series

    try:
        if not old_path.exists():
            c.execute('UPDATE books SET status = ? WHERE id = ?', ('missing', book_id))
            conn.commit()
            return jsonify({'success': False, 'error': 'Source path no longer exists'}), 400

        # Create target directory if needed
        new_path.parent.mkdir(parents=True, exist_ok=True)

        # Move the folder
        import shutil
        shutil.move(str(old_path), str(new_path))

        # Update database
        c.execute('''UPDATE books SET
                     path = ?,
                     current_author = ?,
                     current_title = ?,
                     status = 'fixed'
                     WHERE id = ?''',
                  (str(new_path), detected_author, detected_series, book_id))
        conn.commit()

        logger.info(f"Fixed reversed structure: {old_path} -> {new_path}")

        return jsonify({
            'success': True,
            'old_path': str(old_path),
            'new_path': str(new_path),
            'author': detected_author,
            'title': detected_series
        })

    except Exception as e:
        logger.error(f"Failed to fix reversed structure: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/worker/start', methods=['POST'])
def api_start_worker():
    """Start background worker."""
    start_worker()
    return jsonify({'success': True})

@app.route('/api/worker/stop', methods=['POST'])
def api_stop_worker():
    """Stop background worker."""
    stop_worker()
    return jsonify({'success': True})


@app.route('/api/logs')
def api_logs():
    """Get recent log entries."""
    try:
        log_file = BASE_DIR / 'app.log'
        if log_file.exists():
            with open(log_file, 'r') as f:
                # Read last 100 lines
                lines = f.readlines()[-100:]
                return jsonify({'logs': [line.strip() for line in lines]})
        return jsonify({'logs': []})
    except Exception as e:
        return jsonify({'logs': [f'Error reading logs: {e}']})


@app.route('/api/clear_history', methods=['POST'])
def api_clear_history():
    """Clear all history entries."""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('DELETE FROM history')
        conn.commit()
        conn.close()
        logger.info("History cleared by user")
        return jsonify({'success': True, 'message': 'History cleared'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/recent_history')
def api_recent_history():
    """Get recent history items for live updates."""
    conn = get_db()
    c = conn.cursor()
    # Use LEFT JOIN in case book was deleted or book_id is NULL
    c.execute('''SELECT h.*, b.path FROM history h
                 LEFT JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC LIMIT 15''')
    items = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({'items': items})


@app.route('/api/orphans')
def api_orphans():
    """Find orphan audio files (files sitting directly in author folders)."""
    config = load_config()
    orphans = []

    for lib_path in config.get('library_paths', []):
        lib_orphans = find_orphan_audio_files(lib_path)
        orphans.extend(lib_orphans)

    return jsonify({
        'count': len(orphans),
        'orphans': orphans
    })


@app.route('/api/orphans/organize', methods=['POST'])
def api_organize_orphan():
    """Organize orphan files into a proper book folder."""
    data = request.json
    author_path = data.get('author_path')
    book_title = data.get('book_title')
    files = data.get('files', [])

    if not author_path or not book_title or not files:
        return jsonify({'success': False, 'error': 'Missing required fields'})

    config = load_config()
    success, message = organize_orphan_files(author_path, book_title, files, config)

    return jsonify({
        'success': success,
        'message': message
    })


@app.route('/api/orphans/organize_all', methods=['POST'])
def api_organize_all_orphans():
    """Auto-organize all detected orphan files using metadata."""
    config = load_config()
    results = {'organized': 0, 'errors': 0, 'details': []}

    for lib_path in config.get('library_paths', []):
        orphans = find_orphan_audio_files(lib_path)

        for orphan in orphans:
            if orphan['detected_title'] == 'Unknown Album':
                results['errors'] += 1
                results['details'].append(f"Skipped {orphan['author']}: unknown title")
                continue

            success, message = organize_orphan_files(
                orphan['author_path'],
                orphan['detected_title'],
                orphan['files'],
                config
            )

            if success:
                results['organized'] += 1
                results['details'].append(f"Organized: {orphan['author']}/{orphan['detected_title']}")
            else:
                results['errors'] += 1
                results['details'].append(f"Error: {orphan['author']}: {message}")

    return jsonify({
        'success': True,
        'organized': results['organized'],
        'errors': results['errors'],
        'details': results['details'][:20]  # Limit details
    })


@app.route('/api/library')
def api_library():
    """
    Unified library view - returns ALL items (books, orphans, pending fixes, queue)
    with filter counts. This powers the unified Library page.
    """
    config = load_config()
    conn = get_db()
    c = conn.cursor()

    # Get filter and pagination params
    status_filter = request.args.get('filter', 'all')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    offset = (page - 1) * per_page

    items = []

    # === COUNTS for filter chips ===
    counts = {
        'all': 0,
        'pending': 0,
        'orphan': 0,
        'queue': 0,
        'fixed': 0,
        'verified': 0,
        'error': 0,
        'attention': 0
    }

    # Count books by status
    c.execute("SELECT COUNT(*) FROM books")
    counts['all'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM history WHERE status = 'pending_fix'")
    counts['pending'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM queue")
    counts['queue'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM history WHERE status = 'fixed'")
    counts['fixed'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM books WHERE status = 'verified'")
    counts['verified'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM history WHERE status IN ('error', 'duplicate', 'corrupt_dest')")
    counts['error'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM books WHERE status IN ('needs_attention', 'structure_reversed')")
    c.execute("SELECT COUNT(*) FROM history WHERE status = 'needs_attention'")
    attention_history = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM books WHERE status IN ('needs_attention', 'structure_reversed')")
    attention_books = c.fetchone()[0]
    counts['attention'] = attention_history + attention_books

    # Count orphans (detected on-the-fly)
    orphan_list = []
    for lib_path in config.get('library_paths', []):
        orphan_list.extend(find_orphan_audio_files(lib_path))
    counts['orphan'] = len(orphan_list)

    # Update 'all' count to include orphans
    counts['all'] += counts['orphan']

    # === FETCH ITEMS based on filter ===
    if status_filter == 'orphan':
        # Return orphans as items
        for idx, orphan in enumerate(orphan_list[offset:offset + per_page]):
            items.append({
                'id': f'orphan_{idx}',
                'type': 'orphan',
                'author': orphan['author'],
                'title': orphan['detected_title'],
                'path': orphan['author_path'],
                'status': 'orphan',
                'file_count': orphan['file_count'],
                'files': orphan['files'],
                'author_path': orphan['author_path']
            })

    elif status_filter == 'pending':
        # Items with pending fixes
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path, b.current_author, b.current_title
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'pending_fix'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'pending_fix',
                'book_id': row['book_id'],
                'author': row['old_author'],
                'title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': 'pending_fix',
                'fixed_at': row['fixed_at']
            })

    elif status_filter == 'queue':
        # Items in the processing queue
        c.execute('''SELECT q.id as queue_id, q.reason, q.added_at, q.priority,
                            b.id as book_id, b.path, b.current_author, b.current_title, b.status
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     ORDER BY q.priority, q.added_at
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['queue_id'],
                'type': 'queue',
                'book_id': row['book_id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': 'in_queue',
                'reason': row['reason'],
                'priority': row['priority'],
                'added_at': row['added_at']
            })

    elif status_filter == 'fixed':
        # Successfully fixed items
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at,
                            b.path
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'fixed'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'fixed',
                'book_id': row['book_id'],
                'author': row['new_author'],
                'title': row['new_title'],
                'old_author': row['old_author'],
                'old_title': row['old_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': 'fixed',
                'fixed_at': row['fixed_at']
            })

    elif status_filter == 'verified':
        # Verified/OK books - include profile for source display
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, profile, confidence
                     FROM books
                     WHERE status = 'verified'
                     ORDER BY updated_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            item = {
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': 'verified',
                'confidence': row['confidence'] or 0
            }
            # Parse profile to get verification sources
            if row['profile']:
                try:
                    profile_data = json.loads(row['profile'])
                    # Collect all unique sources used
                    all_sources = set()
                    for field_name in ['author', 'title', 'narrator', 'series']:
                        if field_name in profile_data and profile_data[field_name].get('sources'):
                            all_sources.update(profile_data[field_name]['sources'])
                    item['sources'] = list(all_sources)
                    item['verification_layers'] = profile_data.get('verification_layers_used', [])
                except:
                    pass
            items.append(item)

    elif status_filter == 'error':
        # Error items from history
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status IN ('error', 'duplicate', 'corrupt_dest')
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'error',
                'book_id': row['book_id'],
                'author': row['old_author'],
                'title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': row['status'],
                'error_message': row['error_message'],
                'fixed_at': row['fixed_at']
            })

    elif status_filter == 'attention':
        # Items needing attention
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'needs_attention'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'attention',
                'book_id': row['book_id'],
                'author': row['old_author'],
                'title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'path': row['path'],
                'status': 'needs_attention',
                'error_message': row['error_message']
            })
        # Also get books with structure issues
        c.execute('''SELECT id, path, current_author, current_title, status, error_message
                     FROM books
                     WHERE status IN ('needs_attention', 'structure_reversed')
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book_attention',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'error_message': row['error_message']
            })

    else:  # 'all' - show everything mixed
        # Get recent history items (includes pending, fixed, errors)
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path, b.current_author, b.current_title
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            item_type = 'pending_fix' if row['status'] == 'pending_fix' else \
                        'fixed' if row['status'] == 'fixed' else \
                        'error' if row['status'] in ('error', 'duplicate', 'corrupt_dest') else 'history'
            items.append({
                'id': row['id'],
                'type': item_type,
                'book_id': row['book_id'],
                'author': row['old_author'] if row['status'] == 'pending_fix' else row['new_author'],
                'title': row['old_title'] if row['status'] == 'pending_fix' else row['new_title'],
                'old_author': row['old_author'],
                'old_title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': row['status'],
                'error_message': row['error_message'],
                'fixed_at': row['fixed_at']
            })

    conn.close()

    # Calculate total for pagination
    if status_filter == 'orphan':
        total = counts['orphan']
    elif status_filter == 'pending':
        total = counts['pending']
    elif status_filter == 'queue':
        total = counts['queue']
    elif status_filter == 'fixed':
        total = counts['fixed']
    elif status_filter == 'verified':
        total = counts['verified']
    elif status_filter == 'error':
        total = counts['error']
    elif status_filter == 'attention':
        total = counts['attention']
    else:
        total = counts['all']

    total_pages = (total + per_page - 1) // per_page if total > 0 else 1

    return jsonify({
        'success': True,
        'items': items,
        'counts': counts,
        'filter': status_filter,
        'page': page,
        'per_page': per_page,
        'total': total,
        'total_pages': total_pages,
        'skip_confirmations': config.get('skip_confirmations', False)
    })


@app.route('/library')
def library_page():
    """Unified library view - the main page."""
    config = load_config()
    return render_template('library.html',
                          config=config,
                          worker_running=is_worker_running())


@app.route('/api/settings/skip_confirmations', methods=['POST'])
def api_skip_confirmations():
    """Toggle skip confirmations setting."""
    data = request.json
    value = data.get('value', False)

    config = load_config()
    config['skip_confirmations'] = value
    save_config(config)

    return jsonify({'success': True, 'value': value})


@app.route('/api/version')
def api_version():
    """Return current app version."""
    return jsonify({
        'version': APP_VERSION,
        'repo': GITHUB_REPO
    })

@app.route('/api/check_update')
def api_check_update():
    """Check GitHub for newer version based on update channel."""
    config = load_config()
    channel = config.get('update_channel', 'stable')

    try:
        headers = {'Accept': 'application/vnd.github.v3+json'}

        if channel == 'nightly':
            # Check latest commit on main branch
            url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/main"
            resp = requests.get(url, timeout=5, headers=headers)

            if resp.status_code == 404:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'Repository not found or not published yet'
                })

            if resp.status_code != 200:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'error': f'GitHub API error: {resp.status_code}'
                })

            data = resp.json()
            latest_sha = data.get('sha', '')[:7]
            commit_msg = data.get('commit', {}).get('message', '')[:200]
            commit_date = data.get('commit', {}).get('committer', {}).get('date', '')[:10]
            commit_url = data.get('html_url', '')

            # For nightly, check if we have a local commit hash stored
            local_commit = config.get('local_commit_sha', '')

            return jsonify({
                'update_available': latest_sha != local_commit if local_commit else True,
                'current': APP_VERSION + (f' ({local_commit})' if local_commit else ''),
                'latest': f'main@{latest_sha}',
                'latest_date': commit_date,
                'channel': channel,
                'release_url': commit_url,
                'release_notes': commit_msg,
                'message': 'Tracking latest commits on main branch' if not local_commit else None
            })

        elif channel == 'beta':
            # Check all releases including pre-releases
            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases"
            resp = requests.get(url, timeout=5, headers=headers)

            if resp.status_code == 404:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'No releases found (repo may not be published yet)'
                })

            if resp.status_code != 200:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'error': f'GitHub API error: {resp.status_code}'
                })

            releases = resp.json()
            if not releases:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'No releases found'
                })

            # Get the latest release (first in list, includes pre-releases)
            latest = releases[0]
            latest_version = latest.get('tag_name', '').lstrip('v')
            release_url = latest.get('html_url', '')
            release_notes = latest.get('body', '')[:500]
            is_prerelease = latest.get('prerelease', False)

            update_available = _compare_versions(APP_VERSION, latest_version)

            return jsonify({
                'update_available': update_available,
                'current': APP_VERSION,
                'latest': latest_version + (' (beta)' if is_prerelease else ''),
                'channel': channel,
                'release_url': release_url,
                'release_notes': release_notes if update_available else None
            })

        else:  # stable (default)
            # Check only stable releases (not pre-releases)
            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            resp = requests.get(url, timeout=5, headers=headers)

            if resp.status_code == 404:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'No releases found (repo may not be published yet)'
                })

            if resp.status_code != 200:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'error': f'GitHub API error: {resp.status_code}'
                })

            data = resp.json()
            latest_version = data.get('tag_name', '').lstrip('v')
            release_url = data.get('html_url', '')
            release_notes = data.get('body', '')[:500]

            update_available = _compare_versions(APP_VERSION, latest_version)

            return jsonify({
                'update_available': update_available,
                'current': APP_VERSION,
                'latest': latest_version,
                'channel': channel,
                'release_url': release_url,
                'release_notes': release_notes if update_available else None
            })

    except Exception as e:
        logger.debug(f"Update check failed: {e}")
        return jsonify({
            'update_available': False,
            'current': APP_VERSION,
            'channel': channel,
            'error': str(e)
        })

def _compare_versions(current, latest):
    """Compare semantic versions. Returns True if latest > current."""
    def parse_version(v):
        # Handle versions like "1.0.0-beta.1"
        import re
        match = re.match(r'(\d+)\.(\d+)\.(\d+)', v)
        if match:
            return tuple(int(x) for x in match.groups())
        return (0, 0, 0)

    return parse_version(latest) > parse_version(current)

@app.route('/api/perform_update', methods=['POST'])
def api_perform_update():
    """Perform a git pull to update the application."""
    import subprocess

    # Get the app directory (where this script is located)
    app_dir = os.path.dirname(os.path.abspath(__file__))

    # Determine target branch based on update channel
    config = load_config()
    channel = config.get('update_channel', 'stable')
    target_branch = {
        'stable': 'main',
        'beta': 'develop',
        'nightly': 'develop'  # nightly also uses develop for now
    }.get(channel, 'main')

    try:
        # First check if we're in a git repo
        result = subprocess.run(
            ['git', 'rev-parse', '--git-dir'],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            return jsonify({
                'success': False,
                'error': 'Not a git repository. Manual update required.',
                'instructions': 'Download the latest release from GitHub and replace your installation.'
            })

        # Get current commit before update
        before = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=app_dir, capture_output=True, text=True, timeout=10
        ).stdout.strip()

        # Fetch all branches
        fetch_result = subprocess.run(
            ['git', 'fetch', '--all'],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=60
        )

        # Checkout the target branch based on update channel
        checkout_result = subprocess.run(
            ['git', 'checkout', target_branch],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=30
        )

        if checkout_result.returncode != 0:
            return jsonify({
                'success': False,
                'error': f'Failed to switch to {target_branch} branch',
                'details': checkout_result.stderr or checkout_result.stdout,
                'instructions': f'You may have local changes. Try: git stash && git checkout {target_branch}'
            })

        # Pull latest from target branch
        pull_result = subprocess.run(
            ['git', 'pull', 'origin', target_branch, '--ff-only'],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=60
        )

        # Get new commit after update
        after = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=app_dir, capture_output=True, text=True, timeout=10
        ).stdout.strip()

        if pull_result.returncode != 0:
            return jsonify({
                'success': False,
                'error': 'Git pull failed',
                'details': pull_result.stderr or pull_result.stdout,
                'instructions': 'You may have local changes. Try: git stash && git pull'
            })

        updated = before != after

        return jsonify({
            'success': True,
            'updated': updated,
            'before': before,
            'after': after,
            'branch': target_branch,
            'channel': channel,
            'output': pull_result.stdout,
            'message': f'Updated to {channel} ({target_branch})! Restart the app to apply changes.' if updated else f'Already up to date on {channel} ({target_branch}).',
            'restart_required': updated
        })

    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Update timed out. Check your network connection.'
        })
    except Exception as e:
        logger.error(f"Update failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/restart', methods=['POST'])
def api_restart():
    """Restart the application (for systemd managed services)."""
    import subprocess

    try:
        # Check if running under systemd
        ppid = os.getppid()
        result = subprocess.run(['ps', '-p', str(ppid), '-o', 'comm='],
                               capture_output=True, text=True, timeout=5)

        if 'systemd' in result.stdout:
            # We're running under systemd, restart via systemctl
            # This will kill this process, but systemd will restart it
            subprocess.Popen(['sudo', 'systemctl', 'restart', 'library-manager.service'],
                           start_new_session=True)
            return jsonify({
                'success': True,
                'message': 'Restarting via systemd...'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Not running under systemd. Please restart manually.',
                'instructions': 'Stop the current process and start it again.'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/bug_report')
def api_bug_report():
    """Generate a bug report with system info and sanitized config."""
    import platform
    import sys

    config = load_config()

    # Test API connections instead of showing keys
    api_status = {}

    # Gemini
    if config.get('gemini_api_key'):
        try:
            resp = requests.get(
                f"https://generativelanguage.googleapis.com/v1beta/models?key={config['gemini_api_key']}",
                timeout=5
            )
            api_status['gemini'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['gemini'] = 'connection failed'
    else:
        api_status['gemini'] = 'not configured'

    # Google Books
    if config.get('google_books_api_key'):
        try:
            resp = requests.get(
                f"https://www.googleapis.com/books/v1/volumes?q=test&maxResults=1&key={config['google_books_api_key']}",
                timeout=5
            )
            api_status['google_books'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['google_books'] = 'connection failed'
    else:
        api_status['google_books'] = 'not configured'

    # OpenRouter
    if config.get('openrouter_api_key'):
        try:
            resp = requests.get(
                "https://openrouter.ai/api/v1/models",
                headers={"Authorization": f"Bearer {config['openrouter_api_key']}"},
                timeout=5
            )
            api_status['openrouter'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['openrouter'] = 'connection failed'
    else:
        api_status['openrouter'] = 'not configured'

    # Audiobookshelf
    if config.get('abs_url') and config.get('abs_api_token'):
        try:
            resp = requests.get(
                f"{config['abs_url'].rstrip('/')}/api/libraries",
                headers={"Authorization": f"Bearer {config['abs_api_token']}"},
                timeout=5
            )
            api_status['audiobookshelf'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['audiobookshelf'] = 'connection failed'
    else:
        api_status['audiobookshelf'] = 'not configured'

    # BookDB
    bookdb_url = config.get('bookdb_url', 'https://bookdb.deucebucket.com')
    try:
        resp = requests.get(f"{bookdb_url}/health", timeout=5)
        api_status['bookdb'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
    except:
        api_status['bookdb'] = 'connection failed'

    # Build safe config - only include non-sensitive settings
    safe_config = {}
    # Settings that are safe to share (no paths, no keys, no personal info)
    safe_keys = [
        'naming_format', 'series_grouping', 'auto_fix', 'protect_author_changes',
        'scan_interval_hours', 'batch_size', 'max_requests_per_hour',
        'enable_api_lookups', 'enable_ai_verification', 'enable_audio_analysis',
        'deep_scan_mode', 'profile_confidence_threshold', 'skip_confirmations',
        'ai_provider', 'openrouter_model', 'ollama_model', 'update_channel',
        'enable_ebooks', 'embed_metadata', 'library_language'
    ]
    for key in safe_keys:
        if key in config:
            safe_config[key] = config[key]

    # Show library path count, not actual paths
    safe_config['library_paths_count'] = len(config.get('library_paths', []))

    # Get database stats
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as count FROM books')
    total_books = c.fetchone()['count']
    c.execute('SELECT COUNT(*) as count FROM queue')
    queue_size = c.fetchone()['count']
    c.execute('SELECT COUNT(*) as count FROM history')
    history_count = c.fetchone()['count']
    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'error'")
    error_count = c.fetchone()['count']
    conn.close()

    # Get recent error/warning logs - sanitize paths
    log_file = BASE_DIR / 'app.log'
    recent_errors = []
    if log_file.exists():
        with open(log_file, 'r') as f:
            lines = f.readlines()[-200:]
            for line in lines:
                if 'ERROR' in line or 'WARNING' in line:
                    # Sanitize paths - replace anything that looks like a full path
                    sanitized = re.sub(r'/[a-zA-Z0-9_\-./]+/([^/\s]+)', r'[path]/\1', line.strip())
                    # Also sanitize Windows paths
                    sanitized = re.sub(r'[A-Z]:\\[a-zA-Z0-9_\-\\]+\\([^\\]+)', r'[path]\\\1', sanitized)
                    recent_errors.append(sanitized)
            recent_errors = recent_errors[-30:]

    # Build report
    report = f"""## Bug Report - Library Manager

### System Info
- **Python:** {sys.version.split()[0]}
- **Platform:** {platform.system()} {platform.release()}
- **App Version:** {APP_VERSION}

### API Connection Status
- **Gemini:** {api_status['gemini']}
- **Google Books:** {api_status['google_books']}
- **OpenRouter:** {api_status['openrouter']}
- **Audiobookshelf:** {api_status['audiobookshelf']}
- **BookDB:** {api_status['bookdb']}

### Configuration
```json
{json.dumps(safe_config, indent=2)}
```

### Database Stats
- Total Books: {total_books}
- Queue Size: {queue_size}
- History Entries: {history_count}
- Books with Errors: {error_count}

### Recent Errors/Warnings (paths sanitized)
```
{chr(10).join(recent_errors) if recent_errors else 'No recent errors'}
```

### Description
[Please describe the issue you're experiencing]

### Steps to Reproduce
1. [First step]
2. [Second step]
3. [What happened vs what you expected]
"""

    return jsonify({'report': report})


# ============== OLLAMA INTEGRATION ==============

@app.route('/api/test_ollama', methods=['POST'])
def api_test_ollama():
    """Test connection to Ollama server."""
    data = request.get_json() or {}
    ollama_url = data.get('ollama_url', 'http://localhost:11434').strip().rstrip('/')

    result = test_ollama_connection({'ollama_url': ollama_url})
    return jsonify(result)


@app.route('/api/ollama_models', methods=['POST'])
def api_ollama_models():
    """Get list of available models from Ollama server."""
    data = request.get_json() or {}
    ollama_url = data.get('ollama_url', 'http://localhost:11434').strip().rstrip('/')

    models = get_ollama_models({'ollama_url': ollama_url})
    if models:
        return jsonify({
            'success': True,
            'models': models
        })
    else:
        return jsonify({
            'success': False,
            'models': [],
            'error': 'Could not fetch models from Ollama server'
        })


# ============== API CONNECTION TESTING ==============

@app.route('/api/test_bookdb', methods=['POST'])
def api_test_bookdb():
    """Test connection to BookDB."""
    config = load_config()
    bookdb_url = config.get('bookdb_url', 'https://bookdb.deucebucket.com')

    try:
        resp = requests.get(f"{bookdb_url}/stats", timeout=5)
        if resp.status_code == 200:
            try:
                data = resp.json()
                return jsonify({
                    'success': True,
                    'books_count': data.get('books', 0),
                    'authors_count': data.get('authors', 0),
                    'series_count': data.get('series', 0),
                    'message': f"Connected - {data.get('books', 0):,} books, {data.get('authors', 0):,} authors"
                })
            except:
                return jsonify({
                    'success': False,
                    'error': 'BookDB responded but returned invalid data'
                })
        else:
            return jsonify({
                'success': False,
                'error': f'BookDB returned status {resp.status_code}'
            })
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Cannot connect to BookDB - is it running?'
        })
    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'BookDB connection timed out'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'BookDB error: {str(e)}'
        })


@app.route('/api/test_gemini', methods=['POST'])
def api_test_gemini():
    """Test Gemini API connection."""
    config = load_config()
    api_key = config.get('gemini_api_key', '')

    if not api_key:
        return jsonify({
            'success': False,
            'error': 'No Gemini API key configured'
        })

    try:
        model = config.get('gemini_model', 'gemini-2.0-flash')
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": "Reply with just the word 'connected'"}]}]},
            timeout=10
        )

        if resp.status_code == 200:
            return jsonify({
                'success': True,
                'model': model,
                'message': 'Gemini API connected'
            })
        else:
            error_msg = resp.json().get('error', {}).get('message', f'Status {resp.status_code}')
            return jsonify({
                'success': False,
                'error': error_msg
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/test_openrouter', methods=['POST'])
def api_test_openrouter():
    """Test OpenRouter API connection."""
    config = load_config()
    api_key = config.get('openrouter_api_key', '')

    if not api_key:
        return jsonify({
            'success': False,
            'error': 'No OpenRouter API key configured'
        })

    try:
        model = config.get('openrouter_model', 'google/gemma-3n-e4b-it:free')
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": "Reply with just the word 'connected'"}],
                "max_tokens": 10
            },
            timeout=10
        )

        if resp.status_code == 200:
            return jsonify({
                'success': True,
                'model': model,
                'message': 'OpenRouter API connected'
            })
        else:
            error_data = resp.json()
            error_msg = error_data.get('error', {}).get('message', f'Status {resp.status_code}')
            return jsonify({
                'success': False,
                'error': error_msg
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/reset_database', methods=['POST'])
def api_reset_database():
    """Reset the database and trigger a fresh scan."""
    try:
        conn = get_db()
        c = conn.cursor()

        # Clear all tables
        c.execute('DELETE FROM books')
        c.execute('DELETE FROM queue')
        c.execute('DELETE FROM history')
        c.execute('DELETE FROM stats')

        conn.commit()
        conn.close()

        # Trigger a scan
        config = load_config()
        if config.get('library_paths'):
            import threading
            def run_scan():
                scan_library(config)
            threading.Thread(target=run_scan, daemon=True).start()

        return jsonify({
            'success': True,
            'message': 'Database reset. Scan started.'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


# ============== AUDIOBOOKSHELF INTEGRATION ==============

def get_abs_client():
    """Get configured ABS client or None if not configured."""
    from abs_client import ABSClient
    config = load_config()
    abs_url = config.get('abs_url', '').strip()
    abs_token = config.get('abs_api_token', '').strip()
    if abs_url and abs_token:
        return ABSClient(abs_url, abs_token)
    return None


@app.route('/abs')
def abs_dashboard():
    """ABS integration dashboard - user progress tracking."""
    config = load_config()
    abs_connected = bool(config.get('abs_url') and config.get('abs_api_token'))
    return render_template('abs_dashboard.html',
                           config=config,
                           abs_connected=abs_connected,
                           version=APP_VERSION)


@app.route('/api/abs/test', methods=['POST'])
def api_abs_test():
    """Test ABS connection."""
    data = request.get_json() or {}
    abs_url = data.get('url', '').strip()
    abs_token = data.get('token', '').strip()

    if not abs_url or not abs_token:
        return jsonify({'success': False, 'error': 'URL and API token required'})

    from abs_client import ABSClient
    client = ABSClient(abs_url, abs_token)
    result = client.test_connection()
    return jsonify(result)


@app.route('/api/abs/connect', methods=['POST'])
def api_abs_connect():
    """Save ABS connection settings."""
    data = request.get_json() or {}
    abs_url = data.get('url', '').strip()
    abs_token = data.get('token', '').strip()

    if not abs_url or not abs_token:
        return jsonify({'success': False, 'error': 'URL and API token required'})

    # Test connection first
    from abs_client import ABSClient
    client = ABSClient(abs_url, abs_token)
    result = client.test_connection()

    if result.get('success'):
        # Save URL to config
        config = load_config()
        config['abs_url'] = abs_url
        save_config(config)

        # Save token to secrets (preserving existing secrets)
        secrets = load_secrets()
        secrets['abs_api_token'] = abs_token
        save_secrets(secrets)

        return jsonify({'success': True, 'message': f"Connected as {result.get('username')}"})
    else:
        return jsonify({'success': False, 'error': result.get('error', 'Connection failed')})


@app.route('/api/abs/users')
def api_abs_users():
    """Get all ABS users."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    users = client.get_users()
    return jsonify({
        'success': True,
        'users': [{'id': u.id, 'username': u.username, 'type': u.type} for u in users]
    })


@app.route('/api/abs/libraries')
def api_abs_libraries():
    """Get all ABS libraries."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    libraries = client.get_libraries()
    return jsonify({'success': True, 'libraries': libraries})


@app.route('/api/abs/library/<library_id>/progress')
def api_abs_library_progress(library_id):
    """Get all items in library with user progress."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    items = client.get_library_with_all_progress(library_id)

    # Simplify for JSON
    result = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        result.append({
            'id': item.get('id'),
            'title': metadata.get('title', 'Unknown'),
            'author': metadata.get('authorName', 'Unknown'),
            'duration': media.get('duration', 0),
            'user_progress': item.get('user_progress', {}),
            'progress_summary': item.get('progress_summary', {})
        })

    return jsonify({'success': True, 'items': result})


@app.route('/api/abs/archivable/<library_id>')
def api_abs_archivable(library_id):
    """Get items safe to archive (everyone finished, no one in progress)."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    min_users = request.args.get('min_users', 1, type=int)
    items = client.get_archivable_items(library_id, min_users_finished=min_users)

    result = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        result.append({
            'id': item.get('id'),
            'title': metadata.get('title', 'Unknown'),
            'author': metadata.get('authorName', 'Unknown'),
            'users_finished': item.get('progress_summary', {}).get('users_finished', 0)
        })

    return jsonify({'success': True, 'items': result, 'count': len(result)})


@app.route('/api/abs/untouched/<library_id>')
def api_abs_untouched(library_id):
    """Get items no one has started."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    items = client.get_untouched_items(library_id)

    result = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        result.append({
            'id': item.get('id'),
            'title': metadata.get('title', 'Unknown'),
            'author': metadata.get('authorName', 'Unknown'),
            'added_at': item.get('addedAt')
        })

    return jsonify({'success': True, 'items': result, 'count': len(result)})


# ============== USER GROUPS (for ABS progress rules) ==============

GROUPS_PATH = DATA_DIR / 'user_groups.json'

DEFAULT_GROUPS_DATA = {
    'user_groups': [],      # Groups of ABS users (e.g., "Twilight Readers": [wife, daughter1, daughter2])
    'rules': [],            # Archive rules tied to user groups
    'author_assignments': {},  # author_name -> group_id (smart assign)
    'genre_assignments': {},   # genre -> group_id (smart assign)
    'keep_forever': {          # Never flag these for archive
        'items': [],           # specific item IDs
        'authors': [],         # author names
        'series': []           # series names
    },
    'exclude_from_rules': {    # Exclude from auto-rules (but can still manually archive)
        'authors': [],
        'genres': []
    }
}


def load_groups():
    """Load user groups configuration."""
    if GROUPS_PATH.exists():
        try:
            with open(GROUPS_PATH) as f:
                data = json.load(f)
                # Merge with defaults for any missing keys
                for key, default in DEFAULT_GROUPS_DATA.items():
                    if key not in data:
                        data[key] = default
                return data
        except:
            pass
    return DEFAULT_GROUPS_DATA.copy()


def save_groups(groups):
    """Save user groups configuration."""
    with open(GROUPS_PATH, 'w') as f:
        json.dump(groups, f, indent=2)


@app.route('/api/abs/groups')
def api_abs_groups():
    """Get all groups."""
    return jsonify(load_groups())


@app.route('/api/abs/groups/user', methods=['POST'])
def api_abs_create_user_group():
    """Create a user group."""
    data = request.get_json() or {}
    name = data.get('name', '').strip()
    user_ids = data.get('user_ids', [])

    if not name:
        return jsonify({'success': False, 'error': 'Group name required'})

    groups = load_groups()
    groups['user_groups'].append({
        'id': str(len(groups['user_groups']) + 1),
        'name': name,
        'user_ids': user_ids
    })
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/groups/user/<group_id>', methods=['DELETE'])
def api_abs_delete_user_group(group_id):
    """Delete a user group."""
    groups = load_groups()
    groups['user_groups'] = [g for g in groups['user_groups'] if g['id'] != group_id]
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/groups/rule', methods=['POST'])
def api_abs_create_rule():
    """Create an archive rule.

    Example rule:
    {
        "name": "Archive when family done",
        "user_group_id": "1",  # which users must finish
        "action": "archive",   # what to do
        "enabled": true
    }
    """
    data = request.get_json() or {}

    groups = load_groups()
    groups['rules'].append({
        'id': str(len(groups['rules']) + 1),
        'name': data.get('name', 'Unnamed Rule'),
        'user_group_id': data.get('user_group_id'),
        'action': data.get('action', 'archive'),
        'enabled': data.get('enabled', True)
    })
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/check_rules/<library_id>')
def api_abs_check_rules(library_id):
    """Check which items match archive rules (with smart assignments)."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    groups = load_groups()
    items = client.get_library_with_all_progress(library_id)

    # Build lookups
    user_groups = {g['id']: set(g['user_ids']) for g in groups.get('user_groups', [])}
    author_assignments = groups.get('author_assignments', {})
    genre_assignments = groups.get('genre_assignments', {})
    keep_forever = groups.get('keep_forever', {})
    exclude_from_rules = groups.get('exclude_from_rules', {})

    matches = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        item_id = item.get('id')
        title = metadata.get('title', 'Unknown')
        author = metadata.get('authorName', 'Unknown')
        genres = metadata.get('genres', [])
        series_name = metadata.get('seriesName', '')

        # Check keep forever - skip if protected
        if item_id in keep_forever.get('items', []):
            continue
        if author.lower() in [a.lower() for a in keep_forever.get('authors', [])]:
            continue
        if series_name and series_name.lower() in [s.lower() for s in keep_forever.get('series', [])]:
            continue

        # Check exclude from rules
        if author.lower() in [a.lower() for a in exclude_from_rules.get('authors', [])]:
            continue
        if any(g.lower() in [eg.lower() for eg in exclude_from_rules.get('genres', [])] for g in genres):
            continue

        # Determine which group should handle this item (smart assignment)
        assigned_group_id = None

        # Check author assignment first
        for assigned_author, group_id in author_assignments.items():
            if assigned_author.lower() in author.lower():
                assigned_group_id = group_id
                break

        # Check genre assignment if no author match
        if not assigned_group_id:
            for genre in genres:
                if genre.lower() in [g.lower() for g in genre_assignments.keys()]:
                    assigned_group_id = genre_assignments.get(genre)
                    break

        # Check all rules (both assigned and general)
        user_progress = item.get('user_progress', {})
        finished_users = {uid for uid, p in user_progress.items() if p.get('is_finished')}

        for rule in groups.get('rules', []):
            if not rule.get('enabled'):
                continue

            rule_group_id = rule.get('user_group_id')
            group_users = user_groups.get(rule_group_id, set())

            if not group_users:
                continue

            # If item has smart assignment, only use that group's rules
            if assigned_group_id and rule_group_id != assigned_group_id:
                continue

            # Check if all group members finished
            if group_users.issubset(finished_users):
                matches.append({
                    'rule_name': rule.get('name'),
                    'action': rule.get('action'),
                    'item_id': item_id,
                    'title': title,
                    'author': author,
                    'smart_assigned': assigned_group_id is not None
                })
                break  # Only match one rule per item

    return jsonify({'success': True, 'matches': matches, 'count': len(matches)})


# ============== SMART ASSIGNMENTS ==============

@app.route('/api/abs/assign/author', methods=['POST'])
def api_abs_assign_author():
    """Assign an author to a user group for smart rules."""
    data = request.get_json() or {}
    author = data.get('author', '').strip()
    group_id = data.get('group_id', '').strip()

    if not author or not group_id:
        return jsonify({'success': False, 'error': 'Author and group_id required'})

    groups = load_groups()
    groups['author_assignments'][author] = group_id
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Assigned "{author}" to group'})


@app.route('/api/abs/assign/author/<author>', methods=['DELETE'])
def api_abs_unassign_author(author):
    """Remove author assignment."""
    groups = load_groups()
    if author in groups.get('author_assignments', {}):
        del groups['author_assignments'][author]
        save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/assign/genre', methods=['POST'])
def api_abs_assign_genre():
    """Assign a genre to a user group for smart rules."""
    data = request.get_json() or {}
    genre = data.get('genre', '').strip()
    group_id = data.get('group_id', '').strip()

    if not genre or not group_id:
        return jsonify({'success': False, 'error': 'Genre and group_id required'})

    groups = load_groups()
    groups['genre_assignments'][genre] = group_id
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Assigned genre "{genre}" to group'})


@app.route('/api/abs/assign/genre/<genre>', methods=['DELETE'])
def api_abs_unassign_genre(genre):
    """Remove genre assignment."""
    groups = load_groups()
    if genre in groups.get('genre_assignments', {}):
        del groups['genre_assignments'][genre]
        save_groups(groups)
    return jsonify({'success': True})


# ============== KEEP FOREVER / EXCLUDE ==============

@app.route('/api/abs/keep', methods=['POST'])
def api_abs_keep_forever():
    """Add item/author/series to keep forever list."""
    data = request.get_json() or {}
    item_type = data.get('type')  # 'item', 'author', 'series'
    value = data.get('value', '').strip()

    if not item_type or not value:
        return jsonify({'success': False, 'error': 'Type and value required'})

    groups = load_groups()
    keep = groups.get('keep_forever', {'items': [], 'authors': [], 'series': []})

    if item_type == 'item' and value not in keep['items']:
        keep['items'].append(value)
    elif item_type == 'author' and value not in keep['authors']:
        keep['authors'].append(value)
    elif item_type == 'series' and value not in keep['series']:
        keep['series'].append(value)

    groups['keep_forever'] = keep
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Added to keep forever: {value}'})


@app.route('/api/abs/keep', methods=['DELETE'])
def api_abs_remove_keep():
    """Remove from keep forever list."""
    data = request.get_json() or {}
    item_type = data.get('type')
    value = data.get('value', '').strip()

    groups = load_groups()
    keep = groups.get('keep_forever', {'items': [], 'authors': [], 'series': []})

    if item_type == 'item' and value in keep['items']:
        keep['items'].remove(value)
    elif item_type == 'author' and value in keep['authors']:
        keep['authors'].remove(value)
    elif item_type == 'series' and value in keep['series']:
        keep['series'].remove(value)

    groups['keep_forever'] = keep
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/exclude', methods=['POST'])
def api_abs_exclude():
    """Add author/genre to exclude from auto-rules."""
    data = request.get_json() or {}
    item_type = data.get('type')  # 'author', 'genre'
    value = data.get('value', '').strip()

    if not item_type or not value:
        return jsonify({'success': False, 'error': 'Type and value required'})

    groups = load_groups()
    exclude = groups.get('exclude_from_rules', {'authors': [], 'genres': []})

    if item_type == 'author' and value not in exclude['authors']:
        exclude['authors'].append(value)
    elif item_type == 'genre' and value not in exclude['genres']:
        exclude['genres'].append(value)

    groups['exclude_from_rules'] = exclude
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Excluded from rules: {value}'})


@app.route('/api/abs/exclude', methods=['DELETE'])
def api_abs_remove_exclude():
    """Remove from exclude list."""
    data = request.get_json() or {}
    item_type = data.get('type')
    value = data.get('value', '').strip()

    groups = load_groups()
    exclude = groups.get('exclude_from_rules', {'authors': [], 'genres': []})

    if item_type == 'author' and value in exclude['authors']:
        exclude['authors'].remove(value)
    elif item_type == 'genre' and value in exclude['genres']:
        exclude['genres'].remove(value)

    groups['exclude_from_rules'] = exclude
    save_groups(groups)
    return jsonify({'success': True})


# ============== MANUAL BOOK MATCHING ==============

# Use the public BookBucket API - same as metadata pipeline
# No API key required - the search endpoints are public

@app.route('/api/search_bookdb')
def api_search_bookdb():
    """Search BookBucket for books/series to manually match.
    Uses the public /search endpoint - no API key required.
    Falls back to Google Books if BookDB is unavailable or returns no results.
    """
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'all')  # 'books', 'series', or 'all'
    author = request.args.get('author', '').strip()
    limit = min(int(request.args.get('limit', 20)), 50)

    if not query or len(query) < 2:
        return jsonify({'error': 'Query must be at least 2 characters', 'results': []})

    # Extract series number from query (e.g., "Horus Heresy Book 36" or "#36")
    extracted_series_num = None
    extracted_series_name = None
    series_patterns = [
        r'(?:book|#|no\.?|number)\s*(\d+)',  # "Book 36", "#36", "No. 36"
        r'(\d+)(?:st|nd|rd|th)\s+book',       # "36th book"
        r'\b(\d+)\s*[-–]\s*\w',               # "36 - Title" at start
    ]
    for pattern in series_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            extracted_series_num = int(match.group(1))
            break

    # Also try to extract series name (text before "book N" or similar)
    series_name_match = re.match(r'^(.+?)\s+(?:book|#|no\.?)\s*\d+', query, re.IGNORECASE)
    if series_name_match:
        extracted_series_name = series_name_match.group(1).strip()

    bookdb_error = None
    results = []

    # Try BookDB first
    try:
        params = {'q': query, 'limit': limit}
        if author:
            params['author'] = author

        # Use public /search endpoint (no auth required)
        if search_type == 'all':
            endpoint = f"{BOOKDB_API_URL}/search"
        else:
            endpoint = f"{BOOKDB_API_URL}/search/{search_type}"

        # Longer timeout for cold start (embedding model can take 45-60s to load)
        resp = requests.get(endpoint, params=params, timeout=60)

        if resp.status_code == 200:
            results = resp.json()
            # Enrich results with extracted series info if they lack it
            if results and extracted_series_num:
                for result in results:
                    if not result.get('series_position'):
                        result['series_position'] = extracted_series_num
                        result['series_position_source'] = 'extracted_from_query'
                    if not result.get('series_name') and extracted_series_name:
                        result['series_name'] = extracted_series_name
                        result['series_name_source'] = 'extracted_from_query'
            if results:
                return jsonify({
                    'results': results,
                    'count': len(results),
                    'source': 'bookdb',
                    'extracted_series_num': extracted_series_num,
                    'extracted_series_name': extracted_series_name
                })
            # No results from BookDB - will try fallback
            bookdb_error = 'No results found in BookDB'
        else:
            bookdb_error = f'BookDB API error: {resp.status_code}'

    except requests.exceptions.ConnectionError:
        bookdb_error = 'BookDB server temporarily unavailable (not your issue - our server)'
    except requests.exceptions.Timeout:
        bookdb_error = 'BookDB server timeout (not your issue - our server is slow/down)'
    except Exception as e:
        logger.error(f"BookBucket search error: {e}")
        bookdb_error = str(e)

    # Fallback to Google Books
    try:
        logger.info(f"Manual match fallback to Google Books for '{query}' (BookDB: {bookdb_error})")
        config = load_config()
        google_key = config.get('google_books_api_key')

        import urllib.parse
        search_query = query
        if author:
            search_query += f" inauthor:{author}"

        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(search_query)}&maxResults={limit}"
        if google_key:
            url += f"&key={google_key}"

        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('items', [])

            google_results = []
            for item in items:
                vol = item.get('volumeInfo', {})
                authors = vol.get('authors', [])
                if not vol.get('title') or not authors:
                    continue

                # Extract series info from subtitle
                series_name = None
                series_num = None
                subtitle = vol.get('subtitle', '')
                if subtitle:
                    match = re.search(r'^A\s+(.+?)\s+Novel$', subtitle, re.IGNORECASE)
                    if match:
                        series_name = match.group(1)
                    match = re.search(r'Book\s+(\d+)\s+of\s+(.+)', subtitle, re.IGNORECASE)
                    if match:
                        series_num = int(match.group(1))
                        series_name = match.group(2)
                    match = re.search(r'(.+?)\s+(?:Book|#)\s*(\d+)', subtitle, re.IGNORECASE)
                    if match:
                        series_name = match.group(1)
                        series_num = int(match.group(2))

                # Use extracted series info if we didn't find any in the subtitle
                final_series_name = series_name or extracted_series_name
                final_series_num = series_num or extracted_series_num

                google_results.append({
                    'type': 'book',
                    'name': vol.get('title', ''),
                    'title': vol.get('title', ''),
                    'author_name': authors[0] if authors else '',
                    'year_published': vol.get('publishedDate', '')[:4] if vol.get('publishedDate') else None,
                    'series_name': final_series_name,
                    'series_position': final_series_num,
                    'series_position_source': 'extracted_from_query' if (final_series_num and not series_num) else None,
                    'description': vol.get('description', '')[:500] if vol.get('description') else None,
                    'source': 'googlebooks'
                })

            if google_results:
                return jsonify({
                    'results': google_results,
                    'count': len(google_results),
                    'source': 'googlebooks',
                    'fallback_reason': bookdb_error,
                    'extracted_series_num': extracted_series_num,
                    'extracted_series_name': extracted_series_name
                })

    except Exception as e:
        logger.error(f"Google Books fallback error: {e}")

    # Both failed
    return jsonify({
        'error': f'Search unavailable - this is a server issue on our end, not yours. Try again in a few minutes. ({bookdb_error})',
        'results': [],
        'server_issue': True
    })


@app.route('/api/bookdb_stats')
def api_bookdb_stats():
    """Get BookBucket database statistics (book/author/series counts).
    Uses public /stats endpoint - no API key required.
    """
    try:
        resp = requests.get(f"{BOOKDB_API_URL}/stats", timeout=5)
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify({'error': f'BookBucket API error: {resp.status_code}'})
    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/book_detail/<int:book_id>')
def api_book_detail(book_id):
    """
    Get full book details from BookBucket + ABS status.
    Used for hover cards and detail modals.
    Uses public endpoint - no API key required.
    """
    try:
        # Fetch full book details from BookBucket
        resp = requests.get(f"{BOOKDB_API_URL}/book/{book_id}", timeout=10)

        if resp.status_code != 200:
            return jsonify({'error': f'Book not found (status {resp.status_code})'})

        book = resp.json()

        # Try to find matching item in ABS by title/author
        abs_status = []
        include_abs = request.args.get('include_abs', 'false').lower() == 'true'

        if include_abs and book.get('title'):
            try:
                # Get ABS client
                abs_client = get_abs_client()
                if abs_client:
                    # Search ABS by title
                    libraries = abs_client.get_libraries()
                    title_lower = book.get('title', '').lower()
                    author_lower = (book.get('author_name') or '').lower()

                    for lib in libraries:
                        items_data = abs_client.get_library_items(lib['id'], include_progress=False, limit=0)
                        items = items_data.get('results', [])

                        for item in items:
                            media = item.get('media', {})
                            metadata = media.get('metadata', {})
                            item_title = (metadata.get('title') or '').lower()
                            item_author = (metadata.get('authorName') or '').lower()

                            # Simple fuzzy match - title contains search term
                            if title_lower in item_title or item_title in title_lower:
                                # Check author too if we have it
                                if not author_lower or author_lower in item_author or item_author in author_lower:
                                    # Found a match! Get user progress
                                    item_id = item.get('id')
                                    library_with_progress = abs_client.get_library_with_all_progress(lib['id'])

                                    for lib_item in library_with_progress:
                                        if lib_item.get('id') == item_id:
                                            user_progress = lib_item.get('user_progress', {})
                                            for user_id, progress in user_progress.items():
                                                abs_status.append({
                                                    'username': progress.get('username', 'Unknown'),
                                                    'progress': round(progress.get('progress', 0) * 100),
                                                    'is_finished': progress.get('is_finished', False),
                                                    'library_name': lib.get('name', 'Library')
                                                })
                                            break
                                    break
            except Exception as e:
                logger.warning(f"ABS lookup failed: {e}")

        return jsonify({
            'book': book,
            'abs_status': abs_status
        })

    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        logger.error(f"Book detail error: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/author_detail/<int:author_id>')
def api_author_detail(author_id):
    """
    Get author details from BookBucket.
    Used for hover cards on author search results.
    Uses public endpoint - no API key required.
    """
    try:
        resp = requests.get(f"{BOOKDB_API_URL}/author/{author_id}", timeout=10)

        if resp.status_code != 200:
            return jsonify({'error': f'Author not found (status {resp.status_code})'})

        author = resp.json()
        return jsonify({'author': author})

    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        logger.error(f"Author detail error: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/series_detail/<int:series_id>')
def api_series_detail(series_id):
    """
    Get series details from BookBucket.
    Used for hover cards on series search results.
    Uses public endpoint - no API key required.
    """
    try:
        resp = requests.get(f"{BOOKDB_API_URL}/series/{series_id}", timeout=10)

        if resp.status_code != 200:
            return jsonify({'error': f'Series not found (status {resp.status_code})'})

        series = resp.json()
        return jsonify({'series': series})

    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        logger.error(f"Series detail error: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/manual_match', methods=['POST'])
def api_manual_match():
    """
    Save a manual match for a book in the queue.
    Accepts custom author/title OR a selected BookDB result.
    Creates a pending_fix entry in history for review.
    """
    try:
        data = request.get_json() or {}
        queue_id = data.get('queue_id')

        # Manual entry fields
        new_author = data.get('author', '').strip()
        new_title = data.get('title', '').strip()

        # Or BookDB selection
        bookdb_result = data.get('bookdb_result')  # Full result object from search

        if not queue_id:
            return jsonify({'success': False, 'error': 'queue_id required'})

        conn = get_db()
        c = conn.cursor()

        # Get the queue item with book info
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE q.id = ?''', (queue_id,))
        item = c.fetchone()
        if not item:
            conn.close()
            return jsonify({'success': False, 'error': 'Queue item not found'})

        book_id = item['book_id']
        old_path = item['path']
        old_author = item['current_author']
        old_title = item['current_title']

        # Determine new values from BookDB result if provided
        new_series = None
        new_series_num = None
        new_narrator = None
        new_year = None
        if bookdb_result:
            new_author = bookdb_result.get('author_name') or new_author
            new_title = bookdb_result.get('name') or bookdb_result.get('title') or new_title
            new_series = bookdb_result.get('series_name')
            new_series_num = bookdb_result.get('series_position')
            new_year = bookdb_result.get('year_published')

        # Also accept series fields directly (for manual entry without BookDB selection)
        if not new_series and data.get('series_name'):
            new_series = data.get('series_name')
        if not new_series_num and data.get('series_position'):
            new_series_num = data.get('series_position')

        if not new_author or not new_title:
            conn.close()
            return jsonify({'success': False, 'error': 'Author and title required'})

        # Find which library this book belongs to
        config = load_config()
        lib_path = None
        for lp in config.get('library_paths', []):
            lp_path = Path(lp)
            try:
                Path(old_path).relative_to(lp_path)
                lib_path = lp_path
                break
            except ValueError:
                continue

        if lib_path is None:
            lib_path = Path(old_path).parent.parent

        # Build the new path
        new_path = build_new_path(lib_path, new_author, new_title,
                                  series=new_series, series_num=new_series_num,
                                  narrator=new_narrator, year=new_year, config=config)

        if new_path is None:
            conn.close()
            return jsonify({'success': False, 'error': 'Could not build valid path for this metadata'})

        # Delete any existing pending entries for this book
        c.execute("DELETE FROM history WHERE book_id = ? AND status = 'pending_fix'", (book_id,))

        # Insert as pending fix in history (like process_queue does)
        c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status,
                                          new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix', ?, ?, ?, ?, ?, ?)''',
                 (book_id, old_author, old_title,
                  new_author, new_title, old_path, str(new_path),
                  new_narrator, new_series, str(new_series_num) if new_series_num else None,
                  str(new_year) if new_year else None, None, None))

        # Update book status
        c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', book_id))

        # Remove from queue
        c.execute('DELETE FROM queue WHERE id = ?', (queue_id,))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Saved: {old_author}/{old_title} → {new_author}/{new_title}',
            'new_author': new_author,
            'new_title': new_title
        })
    except Exception as e:
        logger.error(f"Error in manual_match: {e}")
        return jsonify({'success': False, 'error': str(e)})


# ============== BACKUP & RESTORE ==============

import zipfile
import io
from datetime import datetime

BACKUP_FILES = ['config.json', 'secrets.json', 'library.db', 'user_groups.json']

@app.route('/api/backup')
def api_backup():
    """Download a backup of all settings and database."""
    try:
        # Create in-memory zip
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for filename in BACKUP_FILES:
                filepath = DATA_DIR / filename  # Use DATA_DIR for persistent files
                if filepath.exists():
                    zf.write(filepath, filename)
                    logger.info(f"Backup: Added {filename}")

            # Add metadata
            metadata = {
                'backup_date': datetime.now().isoformat(),
                'version': APP_VERSION,
                'files': [f for f in BACKUP_FILES if (DATA_DIR / f).exists()]
            }
            zf.writestr('backup_metadata.json', json.dumps(metadata, indent=2))

        zip_buffer.seek(0)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'library_manager_backup_{timestamp}.zip'

        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/restore', methods=['POST'])
def api_restore():
    """Restore settings and database from a backup zip."""
    if 'backup' not in request.files:
        return jsonify({'success': False, 'error': 'No backup file provided'})

    backup_file = request.files['backup']
    if not backup_file.filename.endswith('.zip'):
        return jsonify({'success': False, 'error': 'Backup must be a .zip file'})

    try:
        # Create a timestamped backup of current state first
        current_backup_dir = DATA_DIR / 'backups' / f'pre_restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        current_backup_dir.mkdir(parents=True, exist_ok=True)

        for filename in BACKUP_FILES:
            filepath = DATA_DIR / filename  # Use DATA_DIR for persistent files
            if filepath.exists():
                import shutil
                shutil.copy2(filepath, current_backup_dir / filename)

        # Extract the uploaded backup
        restored = []
        skipped = []

        with zipfile.ZipFile(backup_file, 'r') as zf:
            # Check for metadata
            if 'backup_metadata.json' in zf.namelist():
                meta = json.loads(zf.read('backup_metadata.json'))
                logger.info(f"Restoring backup from {meta.get('backup_date', 'unknown')}")

            for filename in BACKUP_FILES:
                if filename in zf.namelist():
                    # Extract to data directory (persistent)
                    target_path = DATA_DIR / filename
                    with zf.open(filename) as src:
                        with open(target_path, 'wb') as dst:
                            dst.write(src.read())
                    restored.append(filename)
                    logger.info(f"Restored: {filename}")
                else:
                    skipped.append(filename)

        return jsonify({
            'success': True,
            'message': f'Restored {len(restored)} files. Please restart the app to apply changes.',
            'restored': restored,
            'skipped': skipped,
            'pre_restore_backup': str(current_backup_dir)
        })

    except zipfile.BadZipFile:
        return jsonify({'success': False, 'error': 'Invalid zip file'})
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/backup/info')
def api_backup_info():
    """Get info about what would be backed up."""
    files_info = []
    total_size = 0

    for filename in BACKUP_FILES:
        filepath = DATA_DIR / filename  # Use DATA_DIR for persistent files
        if filepath.exists():
            size = filepath.stat().st_size
            total_size += size
            files_info.append({
                'name': filename,
                'size': size,
                'size_human': f'{size / 1024:.1f} KB' if size > 1024 else f'{size} bytes',
                'modified': datetime.fromtimestamp(filepath.stat().st_mtime).isoformat()
            })

    return jsonify({
        'files': files_info,
        'total_size': total_size,
        'total_size_human': f'{total_size / 1024:.1f} KB' if total_size > 1024 else f'{total_size} bytes'
    })


# ============== MAIN ==============

if __name__ == '__main__':
    migrate_legacy_config()  # Migrate from old location if needed (Issue #23)
    init_config()  # Create config files if they don't exist
    init_db()
    start_worker()
    port = int(os.environ.get('PORT', 5757))
    app.run(host='0.0.0.0', port=port, debug=False)