"""Book Profile system - confidence-scored metadata profiles for comprehensive book identification."""

import json
import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path


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


def is_valid_title(title: str) -> bool:
    """
    Validate that a string looks like a real book title, not garbage.
    Prevents bad recommendations with truncated or polluted titles.
    """
    if not title or not isinstance(title, str):
        return False

    title = title.strip()

    # Too short
    if len(title) < 2:
        return False

    # Starts with lowercase fragment (truncated like "ital present")
    if re.match(r'^[a-z]{2,6}\s+', title) and not title[0].isupper():
        return False

    # Famous numeric titles that are valid
    famous_numeric = {'1984', '2001', '2010', '1776', '1066', '1421', '1491', '1493', '11/22/63'}

    # Contains obvious metadata pollution
    pollution_patterns = [
        r'modern library c\.\s*\d{4}',  # "Modern Library c. 1951"
        r'\b(hardcover|paperback|mass market)\b',
        r'\bfirst edition\b',
        r'\b\d{4}\s*(edition|printing|ed\.)\b',
        # Audio intro pollution
        r'\btantor\s+audio\b',  # "Tantor Audio..."
        r'\brecorded\s+books\b',
        r'\bbrilliance\s+audio\b',
        r'\bpodium\s+audio\b',
        r'\bblackstone\s+audio\b',
        r'\baudible\s+studios\b',
        r'\bdivision\s+of\b',
        r',\s*written\s+(and\s+)?read\s+',  # "written and read for you"
        r'\bpresents?\s+',  # "...presents The Book"
    ]
    for pattern in pollution_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            return False

    # Just a number (but allow famous numeric titles)
    if re.match(r'^\s*\d+\s*$', title) and title.strip() not in famous_numeric:
        return False

    # Should contain at least one letter OR be a known numeric title
    if not re.search(r'[a-zA-Z]', title):
        if title.strip() not in famous_numeric:
            return False

    return True


def is_valid_author(author: str) -> bool:
    """
    Validate that a string looks like a real author name, not garbage.
    Prevents bad recommendations like "earth", "[SCAN] Vol 13", "World War I".
    """
    if not author or not isinstance(author, str):
        return False

    author = author.strip()

    # Too short (single word under 3 chars is suspicious)
    if len(author) < 3:
        return False

    # Contains brackets (likely filename garbage like [SCAN], (2018), etc.)
    if re.search(r'[\[\]()]', author):
        return False

    # Starts with numbers (year, series number, etc.)
    if re.match(r'^\d', author):
        return False

    # Is just a year
    if re.match(r'^\d{4}$', author):
        return False

    # Single common word that's clearly not a name
    single_word_blacklist = {
        'earth', 'world', 'war', 'book', 'vol', 'volume', 'part', 'chapter',
        'series', 'saga', 'chronicles', 'collection', 'anthology', 'edition',
        'complete', 'unabridged', 'abridged', 'audio', 'audiobook', 'ebook',
        'scan', 'index', 'contents', 'introduction', 'preface', 'epilogue',
        'don', "don't", 'panic', 'the', 'a', 'an', 'of', 'and', 'or', 'in',
        'unknown', 'various', 'anonymous', 'n/a', 'na', 'none', 'null',
    }
    if author.lower() in single_word_blacklist:
        return False

    # Multi-word phrases that are clearly titles, not authors
    phrase_blacklist = {
        "don't panic", "don t panic", "world war", "the end",
        "the beginning", "the return", "the rise", "the fall",
    }
    if author.lower().strip() in phrase_blacklist:
        return False

    # Looks like a topic rather than person (starts with "The ", "A ", etc.)
    topic_patterns = [
        r'^(the|a|an)\s+\w+\s+(of|and|or|in)\s+',  # "The Nature of..."
        r'^world\s+war',  # "World War I"
        r'^\d+\s+(things|ways|secrets|lessons)',  # "10 Things..."
        r'^(vol|volume|book|part|chapter)\s*\d',  # "Vol 13"
    ]
    for pattern in topic_patterns:
        if re.match(pattern, author.lower()):
            return False

    # Should contain at least one letter
    if not re.search(r'[a-zA-Z]', author):
        return False

    return True


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

    # Skaldleita identification IDs - for instant lookup and tracking
    audio_fingerprint: Optional[str] = None      # Chromaprint fingerprint
    narrator_id: Optional[str] = None            # Narrator voice ID (TBD_xxx or known name)
    book_id: Optional[str] = None                # Book ID (ISBN, ASIN, internal)
    version_id: Optional[str] = None             # Unique recording version ID
    voice_cluster_id: Optional[str] = None       # Voice cluster for unknown narrators

    def add_author(self, source: str, author: str, weight: int = None):
        """Add author source with validation to prevent garbage recommendations.

        Rejects obvious non-authors like 'earth', '[SCAN] Vol 13', 'World War I'.
        """
        if not is_valid_author(author):
            # Don't add garbage - silently reject
            return False
        self.author.add_source(source, author, weight)
        return True

    def add_title(self, source: str, title: str, weight: int = None):
        """Add title source with validation to prevent garbage recommendations.

        Rejects truncated fragments like 'ital present...' and metadata pollution.
        """
        if not is_valid_title(title):
            # Don't add garbage - silently reject
            return False
        self.title.add_source(source, title, weight)
        return True

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
        # Skaldleita IDs
        result['audio_fingerprint'] = self.audio_fingerprint
        result['narrator_id'] = self.narrator_id
        result['book_id'] = self.book_id
        result['version_id'] = self.version_id
        result['voice_cluster_id'] = self.voice_cluster_id
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
        # Skaldleita IDs
        profile.audio_fingerprint = data.get('audio_fingerprint')
        profile.narrator_id = data.get('narrator_id')
        profile.book_id = data.get('book_id')
        profile.version_id = data.get('version_id')
        profile.voice_cluster_id = data.get('voice_cluster_id')
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


# Database getter - will be set by app.py during initialization
_get_db = None

# Narrator saver function - will be set by app.py during initialization
_save_narrator = None


def set_db_getter(func):
    """Set the database connection getter function."""
    global _get_db
    _get_db = func


def set_narrator_saver(func):
    """Set the narrator auto-save function."""
    global _save_narrator
    _save_narrator = func


def save_book_profile(book_id: int, profile: BookProfile):
    """Save a book profile to the database."""
    if _get_db is None:
        raise RuntimeError("Database getter not initialized. Call set_db_getter() first.")
    conn = _get_db()
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
    if _get_db is None:
        raise RuntimeError("Database getter not initialized. Call set_db_getter() first.")
    conn = _get_db()
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
    audio_result: dict = None,
    fingerprint_data: dict = None
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
            profile.add_author('path', path_info['detected_author'])
        if path_info.get('detected_title'):
            profile.add_title('path', path_info['detected_title'])
        if path_info.get('detected_series'):
            profile.series.add_source('path', path_info['detected_series'])
        if path_info.get('issues'):
            profile.issues.extend(path_info['issues'])

    # Layer 1: Folder metadata (ID3, NFO, JSON)
    if folder_meta:
        if folder_meta.get('audio_author'):
            profile.add_author('id3', folder_meta['audio_author'])
        if folder_meta.get('audio_title'):
            profile.add_title('id3', folder_meta['audio_title'])
        if folder_meta.get('nfo_author'):
            profile.add_author('nfo', folder_meta['nfo_author'])
        if folder_meta.get('nfo_title'):
            profile.add_title('nfo', folder_meta['nfo_title'])
        if folder_meta.get('nfo_narrator'):
            profile.narrator.add_source('nfo', folder_meta['nfo_narrator'])
            if _save_narrator:
                _save_narrator(folder_meta['nfo_narrator'], source='nfo_extract')
        if folder_meta.get('meta_author'):
            profile.add_author('json', folder_meta['meta_author'])
        if folder_meta.get('meta_title'):
            profile.add_title('json', folder_meta['meta_title'])
        if folder_meta.get('meta_narrator'):
            profile.narrator.add_source('json', folder_meta['meta_narrator'])
            if _save_narrator:
                _save_narrator(folder_meta['meta_narrator'], source='json_extract')

    # Layer 2: API candidates
    if api_candidates:
        if 'api' not in profile.verification_layers_used:
            profile.verification_layers_used.append('api')
        for candidate in api_candidates:
            source = candidate.get('source', 'api')
            if candidate.get('author'):
                profile.add_author(source, candidate['author'])
            if candidate.get('title'):
                profile.add_title(source, candidate['title'])
            if candidate.get('series'):
                profile.series.add_source(source, candidate['series'])
            if candidate.get('series_num'):
                profile.series_num.add_source(source, candidate['series_num'])
            if candidate.get('year'):
                profile.year.add_source(source, candidate['year'])

    # Layer 3: AI result
    if ai_result:
        if 'ai' not in profile.verification_layers_used:
            profile.verification_layers_used.append('ai')
        if ai_result.get('author'):
            profile.add_author('ai', ai_result['author'])
        if ai_result.get('title'):
            profile.add_title('ai', ai_result['title'])
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
            profile.add_author('audio', audio_result['author'])
        if audio_result.get('title'):
            profile.add_title('audio', audio_result['title'])
        if audio_result.get('narrator'):
            profile.narrator.add_source('audio', audio_result['narrator'])
        if audio_result.get('series'):
            profile.series.add_source('audio', audio_result['series'])
        if audio_result.get('language'):
            profile.language.add_source('audio', audio_result['language'])

    # Skaldleita fingerprint/voice data
    if fingerprint_data:
        if 'fingerprint' not in profile.verification_layers_used:
            profile.verification_layers_used.append('fingerprint')
        if fingerprint_data.get('audio_fingerprint'):
            profile.audio_fingerprint = fingerprint_data['audio_fingerprint']
        if fingerprint_data.get('narrator_id'):
            profile.narrator_id = fingerprint_data['narrator_id']
        if fingerprint_data.get('voice_cluster_id'):
            profile.voice_cluster_id = fingerprint_data['voice_cluster_id']
        if fingerprint_data.get('book_id'):
            profile.book_id = fingerprint_data['book_id']
        if fingerprint_data.get('version_id'):
            profile.version_id = fingerprint_data['version_id']

    # Finalize - calculate consensus values and confidence
    profile.finalize()

    # Flag low confidence for attention
    if profile.overall_confidence < 50:
        profile.needs_attention = True
        if 'low_confidence' not in profile.issues:
            profile.issues.append('low_confidence')

    return profile


__all__ = [
    'SOURCE_WEIGHTS',
    'FIELD_WEIGHTS',
    'FieldValue',
    'BookProfile',
    'detect_multibook_vs_chapters',
    'save_book_profile',
    'load_book_profile',
    'build_profile_from_sources',
    'set_db_getter',
    'set_narrator_saver',
    'is_valid_author',
    'is_valid_title'
]
