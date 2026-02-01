"""Layer 2: AI Verification Queue Processing

Processes items using AI verification to identify books.
This is the main AI-powered identification layer.
"""

import json
import logging
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple, Type

from library_manager.utils.validation import (
    is_valid_author_for_recommendation, is_valid_title_for_recommendation
)

logger = logging.getLogger(__name__)


# Language detection for multi-language naming
def _detect_title_language(text):
    """Detect language from title text."""
    if not text or len(text) < 3:
        return None
    try:
        from langdetect import detect, DetectorFactory
        DetectorFactory.seed = 0
        return detect(text)
    except Exception:
        return None


def process_queue(
    config: Dict,
    get_db: Callable,
    check_rate_limit: Callable,
    call_ai: Callable,
    detect_multibook_vs_chapters: Callable,
    auto_save_narrator: Callable,
    standardize_initials: Callable,
    extract_series_from_title: Callable,
    is_placeholder_author: Callable,
    build_new_path: Callable,
    is_drastic_author_change: Callable,
    verify_drastic_change: Callable,
    analyze_audio_for_credits: Callable,
    compare_book_folders: Callable,
    sanitize_path_component: Callable,
    extract_narrator_from_folder: Callable,
    build_metadata_for_embedding: Callable,
    embed_tags_for_path: Callable,
    BookProfile: Type,
    audio_extensions: Set[str],
    limit: Optional[int] = None,
    verification_layer: int = 2,
    set_current_book: Optional[Callable] = None
) -> Tuple[int, int]:
    """
    Process items in the queue using AI verification.

    Args:
        config: Configuration dict
        get_db: Function to get database connection
        check_rate_limit: Function to check API rate limits
        call_ai: Function to call AI for identification
        detect_multibook_vs_chapters: Function to detect multi-book folders
        auto_save_narrator: Function to save narrator info
        standardize_initials: Function to standardize author initials
        extract_series_from_title: Function to extract series from title
        is_placeholder_author: Function to check for placeholder authors
        build_new_path: Function to build new paths
        is_drastic_author_change: Function to detect drastic author changes
        verify_drastic_change: Function to verify drastic changes
        analyze_audio_for_credits: Function to analyze audio for credits
        compare_book_folders: Function to compare book folders
        sanitize_path_component: Function to sanitize path components
        extract_narrator_from_folder: Function to extract narrator from folder
        build_metadata_for_embedding: Function to build metadata for embedding
        embed_tags_for_path: Function to embed tags in files
        BookProfile: BookProfile class for creating profiles
        audio_extensions: Set of audio file extensions
        limit: Max items to process
        verification_layer: Which layer's items to process (2=AI, 4=folder fallback)

    Returns:
        Tuple of (processed_count, fixed_count)

    NOTE: This function uses a 3-phase approach to avoid holding DB locks during
    external AI API calls (which can take 5-30+ seconds):
      Phase 1: Quick fetch, release connection
      Phase 2: External AI call (no DB connection held)
      Phase 3: Reconnect and process results
    """
    # Check rate limit first
    allowed, calls_made, max_calls = check_rate_limit(config)
    if not allowed:
        logger.warning(f"Rate limit reached: {calls_made}/{max_calls} calls. Waiting...")
        return 0, 0

    # Check if AI verification is enabled (before opening connection)
    if not config.get('enable_ai_verification', True):
        logger.info("[LAYER 2] AI verification disabled, skipping")
        return 0, 0

    # === PHASE 1: Fetch batch (quick read, release connection immediately) ===
    conn = get_db()
    c = conn.cursor()

    batch_size = config.get('batch_size', 3)
    if limit:
        batch_size = min(batch_size, limit)

    layer_name = "LAYER 2/AI" if verification_layer == 2 else f"LAYER {verification_layer}"
    logger.info(f"[{layer_name}] process_queue called with batch_size={batch_size}, limit={limit}, layer={verification_layer} (API: {calls_made}/{max_calls})")

    # Get batch from queue - process items at specified verification_layer
    # Skip user-locked books - user has manually set metadata
    api_enabled = config.get('enable_api_lookups', True)
    if api_enabled or verification_layer == 4:
        # Process items at specified layer (or layer 4 for folder fallback)
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title,
                            b.confidence, b.profile
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.verification_layer = ?
                       AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                       AND (b.user_locked IS NULL OR b.user_locked = 0)
                     ORDER BY q.priority, q.added_at
                     LIMIT ?''', (verification_layer, batch_size))
    else:
        # API disabled - process all queue items directly with AI
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title,
                            b.confidence, b.profile
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                       AND (b.user_locked IS NULL OR b.user_locked = 0)
                     ORDER BY q.priority, q.added_at
                     LIMIT ?''', (batch_size,))
    # Convert to dicts immediately - sqlite3.Row objects become invalid after conn.close()
    batch = [dict(row) for row in c.fetchall()]
    conn.close()  # Release DB lock BEFORE external AI call

    logger.info(f"[{layer_name}] Fetched {len(batch)} items from queue")

    if not batch:
        logger.info(f"[{layer_name}] No items in batch, returning 0")
        return 0, 0  # (processed, fixed)

    # === GARBAGE INPUT FILTER ===
    # Filter out system folders that slipped through to Layer 2
    # These should NEVER be sent to AI - it will hallucinate
    garbage_inputs = {
        '@eadir', '#recycle', '@syno', '@tmp',
        '.appledouble', '__macosx', '.ds_store', '.spotlight', '.fseventsd', '.trashes',
        '$recycle.bin', 'system volume information', 'thumbs.db',
        '.trash', '.cache', '.metadata', '.thumbnails',
        'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
        'backup', 'backups', '.streams', 'streams'
    }

    clean_batch = []
    garbage_batch = []
    for row in batch:
        title_lower = (row.get('current_title') or '').lower().strip()
        author_lower = (row.get('current_author') or '').lower().strip()

        is_garbage = False
        if title_lower in garbage_inputs or author_lower in garbage_inputs:
            is_garbage = True
        elif title_lower.startswith('@') or title_lower.startswith('#'):
            is_garbage = True
        elif author_lower.startswith('@') or author_lower.startswith('#'):
            is_garbage = True

        if is_garbage:
            logger.info(f"[{layer_name}] REJECTED garbage input (system folder): {author_lower}/{title_lower}")
            garbage_batch.append(row)
        else:
            clean_batch.append(row)

    # Handle garbage items - mark for user cleanup
    if garbage_batch:
        conn_garbage = get_db()
        c_garbage = conn_garbage.cursor()
        for row in garbage_batch:
            c_garbage.execute('''UPDATE books SET status = 'needs_attention',
                                error_message = 'System folder detected - remove from library',
                                verification_layer = 4 WHERE id = ?''', (row['book_id'],))
            c_garbage.execute('DELETE FROM queue WHERE id = ?', (row['id'],))
        conn_garbage.commit()
        conn_garbage.close()
        logger.info(f"[{layer_name}] Rejected {len(garbage_batch)} garbage items")

    batch = clean_batch
    if not batch:
        logger.info(f"[{layer_name}] All items were garbage, nothing to process")
        return len(garbage_batch), 0  # (processed, fixed)

    # Build messy names for AI
    messy_names = [f"{row['current_author']} - {row['current_title']}" for row in batch]

    logger.info(f"[DEBUG] Processing batch of {len(batch)} items:")
    for i, name in enumerate(messy_names):
        logger.info(f"[DEBUG]   Item {i+1}: {name}")

    # === PHASE 2: External AI call (NO database connection held) ===
    results = call_ai(messy_names, config)
    logger.info(f"[DEBUG] AI returned {len(results) if results else 0} results")

    if not results:
        logger.warning("No results from AI")
        return 0, 0  # (processed, fixed)

    # === PHASE 3: Process results and apply DB updates ===
    conn = get_db()
    c = conn.cursor()

    # Update API call stats (INSERT if not exists, then UPDATE to preserve other columns)
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('UPDATE stats SET api_calls = COALESCE(api_calls, 0) + 1 WHERE date = ?', (today,))

    processed = 0
    fixed = 0
    for row, result in zip(batch, results):
        # Issue #86: Validate result is a dict before processing
        # AI can return malformed JSON that parses as string/list/None
        if not isinstance(result, dict):
            logger.warning(f"[{layer_name}] AI returned invalid result type {type(result).__name__} for {row.get('path', 'unknown')} - skipping")
            processed += 1
            continue

        # Update status bar with current book
        if set_current_book:
            set_current_book(
                row.get('current_author') or 'Unknown',
                row.get('current_title') or 'Unknown',
                "Verifying with AI..."
            )

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
                           if f.is_file() and f.suffix.lower() in audio_extensions]
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

        # CRITICAL: Catch AI returning literal "None" or "null" as strings
        # This happens when AI misunderstands the JSON format or the book
        null_strings = {'none', 'null', 'n/a', 'unknown', 'untitled', ''}
        if new_title.lower() in null_strings:
            logger.warning(f"AI returned invalid title '{new_title}' - treating as empty")
            new_title = ''
        if new_author.lower() in null_strings:
            logger.warning(f"AI returned invalid author '{new_author}' - treating as empty")
            new_author = ''

        # CRITICAL: Prevent title shortening - if AI returns a substring of the original title,
        # keep the original (more specific) title. Example: "Double Cross" -> "Cross" is WRONG
        # This catches the case where API finds a shorter-titled book in the same series
        current_title = row.get('current_title', '')
        if new_title and current_title:
            new_title_lower = new_title.lower().strip()
            current_title_lower = current_title.lower().strip()
            # If new title is shorter AND is contained in the original title, keep original
            if (len(new_title_lower) < len(current_title_lower) and
                new_title_lower in current_title_lower and
                len(new_title_lower) >= 3):  # Avoid very short matches
                logger.warning(f"AI shortened title '{current_title}' to '{new_title}' - keeping original")
                new_title = current_title

        # CRITICAL: Detect when AI swaps author/title (common when folder has title first)
        # If new_title looks like current_author and new_author looks like current_title, swap them
        if new_author and new_title and row.get('current_author') and row.get('current_title'):
            # Check if AI accidentally swapped them
            current_author_clean = row['current_author'].lower().strip()
            current_title_clean = row['current_title'].lower().strip()
            new_author_clean = new_author.lower().strip()
            new_title_clean = new_title.lower().strip()

            # If new_author matches old_title and new_title matches old_author, AI swapped them
            if (new_author_clean == current_title_clean and new_title_clean == current_author_clean):
                logger.warning(f"AI appears to have swapped author/title, correcting: {new_author}/{new_title} -> {new_title}/{new_author}")
                new_author, new_title = new_title, new_author

        # CRITICAL: Known narrators that AI sometimes mistakes for authors
        # These are famous audiobook narrators, NOT authors
        known_narrators = {
            'scott brick', 'ray porter', 'luke daniels', 'wil wheaton', 'steven pacey',
            'tim gerard reynolds', 'r.c. bray', 'rc bray', 'nick podehl', 'simon vance',
            'michael kramer', 'kate reading', 'january lavoy', 'rebecca soler',
            'kirby heyborne', 'stephen fry', 'rob inglis', 'toby longworth',
            'joe morton', 'bahni turpin', 'robin miles', 'dion graham'
        }
        if new_author.lower() in known_narrators:
            # Narrator mistaken for author - flag for attention
            logger.warning(f"AI returned narrator '{new_author}' as author - flagging for attention")
            c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                     ('needs_attention', f"AI returned narrator '{new_author}' as author - needs manual review", row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            processed += 1
            continue

        # Auto-save narrator to Skaldleita if we found one
        if new_narrator:
            auto_save_narrator(new_narrator, source='ai_extract')

        # Issue #57: Apply author initials standardization if enabled
        # This ensures "Peter F Hamilton" becomes "Peter F. Hamilton" consistently
        if config.get('standardize_author_initials', False) and new_author:
            new_author = standardize_initials(new_author)

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
            # AI couldn't identify the book - decide what to do based on existing verification
            # Check if book was already verified by Layer 1 (has profile with confidence)
            book_confidence = row.get('confidence', 0) or 0
            has_profile = row.get('profile') is not None and row.get('profile') != ''

            if is_placeholder_author(row['current_author']):
                # Placeholder author - advance to Layer 3 for audio analysis
                if config.get('enable_audio_analysis', False):
                    logger.info(f"Advancing to Layer 3 (AI empty, placeholder author '{row['current_author']}'): {row['current_title']}")
                    c.execute('UPDATE books SET verification_layer = 3 WHERE id = ?', (row['book_id'],))
                    conn.commit()
                    processed += 1
                    continue
                else:
                    # Audio analysis disabled - mark as needs_attention
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    logger.info(f"Needs attention (AI empty, placeholder author '{row['current_author']}', no audio): {row['current_title']}")
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('needs_attention', f"Could not identify author (currently '{row['current_author']}')", row['book_id']))
            elif book_confidence >= 40 and has_profile:
                # Book was verified by Layer 1 with decent confidence - trust that verification
                c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
                logger.info(f"Verified OK (Layer 1 verified, AI empty): {row['current_author']}/{row['current_title']} (conf={book_confidence})")
            else:
                # No prior verification AND AI couldn't identify - needs attention, not blind trust
                # The folder name might have typos or be completely wrong
                logger.info(f"Needs attention (AI empty, no prior verification): {row['current_author']}/{row['current_title']}")
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))  # Fix: remove from queue
                c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                         ('needs_attention', f"AI could not verify - folder may have typos or incorrect metadata", row['book_id']))
            processed += 1
            continue

        # Check if fix needed (also check narrator change)
        if new_author != row['current_author'] or new_title != row['current_title'] or new_narrator:
            old_path = Path(row['path'])

            # Find which configured library this book belongs to
            # (Don't assume 2-level structure - series_grouping uses 3 levels)
            lib_path = None
            is_from_watch_folder = False
            for lp in config.get('library_paths', []):
                lp_path = Path(lp)
                try:
                    old_path.relative_to(lp_path)
                    lib_path = lp_path
                    break
                except ValueError:
                    continue

            # Issue #57: Check if book is from watch folder and should go to watch_output_folder
            watch_folder = config.get('watch_folder', '').strip()
            watch_output_folder = config.get('watch_output_folder', '').strip()
            if watch_folder and watch_output_folder:
                try:
                    watch_path = Path(watch_folder).resolve()
                    old_path_resolved = old_path.resolve()
                    try:
                        old_path_resolved.relative_to(watch_path)
                        # Book is in watch folder - use output folder as target
                        lib_path = Path(watch_output_folder)
                        is_from_watch_folder = True
                        logger.info(f"Watch folder book: routing to output folder {lib_path}")
                    except ValueError:
                        pass  # Not in watch folder
                except Exception as e:
                    logger.debug(f"Watch folder path check failed: {e}")

            # Fallback if not found in configured libraries
            if lib_path is None:
                lib_path = old_path.parent.parent
                logger.warning(f"Book path {old_path} not under any configured library, guessing lib_path={lib_path}")

            # Detect language for multi-language naming
            lang_code = _detect_title_language(new_title)
            new_path = build_new_path(lib_path, new_author, new_title,
                                      series=new_series, series_num=new_series_num,
                                      narrator=new_narrator, year=new_year,
                                      edition=new_edition, variant=new_variant,
                                      language_code=lang_code, config=config)

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
                            # Use smart first-file detection for opening credits
                            audio_result = analyze_audio_for_credits(str(old_path), config)

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
                            # Validate before creating pending_fix (Issue #92: prevent garbage recommendations)
                            if not is_valid_author_for_recommendation(new_author):
                                logger.warning(f"[LAYER 2] Rejected garbage author: '{new_author}' for {row['current_title']}")
                                c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                processed += 1
                                continue
                            if not is_valid_title_for_recommendation(new_title):
                                logger.warning(f"[LAYER 2] Rejected garbage title: '{new_title}' for {row['current_author']}")
                                c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                                processed += 1
                                continue
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
                        # Use smart first-file detection for opening credits
                        audio_result = analyze_audio_for_credits(str(old_path), config)
                        if audio_result and audio_result.get('author'):
                            # Use audio result directly
                            new_author = audio_result.get('author', new_author)
                            new_title = audio_result.get('title', new_title)
                            new_narrator = audio_result.get('narrator', new_narrator)
                            drastic_change = is_drastic_author_change(row['current_author'], new_author)
                            logger.info(f"TRUST THE PROCESS: Using audio metadata: {new_author} - {new_title}")
                        else:
                            # Audio failed or no audio files - flag for attention
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
                        # Standard mode - block the change
                        logger.warning(f"BLOCKED (verification failed): {row['current_author']} -> {new_author}")
                        c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        processed += 1
                        continue

                # Recalculate new_path with potentially updated author/title/narrator
                # Detect language for multi-language naming
                lang_code = _detect_title_language(new_title)
                new_path = build_new_path(lib_path, new_author, new_title,
                                          series=new_series, series_num=new_series_num,
                                          narrator=new_narrator, year=new_year,
                                          edition=new_edition, variant=new_variant,
                                          language_code=lang_code, config=config)

                # CRITICAL SAFETY: Check recalculated path
                if new_path is None:
                    logger.error(f"SAFETY BLOCK: Invalid recalculated path for '{new_author}' / '{new_title}'")
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('error', 'Path validation failed after verification', row['book_id']))
                    conn.commit()
                    processed += 1
                    continue

            # Issue #57 fix: Check if book is already in correct location
            # Without this check, we'd compare the folder to itself and mark it as "duplicate"
            if old_path.resolve() == new_path.resolve():
                # Issue #59: If author is placeholder (Unknown, etc.), advance to Layer 3 for audio analysis
                if is_placeholder_author(new_author):
                    # Check if audio analysis is enabled before advancing
                    if config.get('enable_audio_analysis', False):
                        logger.info(f"Advancing to Layer 3 (placeholder author '{new_author}'): {old_path.name}")
                        c.execute('UPDATE books SET verification_layer = 3 WHERE id = ?', (row['book_id'],))
                        conn.commit()
                        processed += 1
                        continue
                    else:
                        # Audio analysis disabled - mark as needs_attention
                        logger.info(f"Needs attention (placeholder author '{new_author}', no audio analysis): {old_path.name}")
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                 ('needs_attention', f"Could not identify author (currently '{new_author}')", row['book_id']))
                else:
                    # Only mark as verified if we have actual confidence in the identification
                    # Otherwise the AI is just echoing back the folder name without verification
                    book_confidence = row.get('confidence', 0) or 0
                    if book_confidence >= 40:
                        logger.info(f"Already correct (conf={book_confidence}): {old_path.name}")
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        c.execute('UPDATE books SET status = ? WHERE id = ?',
                                 ('verified', row['book_id']))
                    else:
                        # Low/no confidence - needs manual verification
                        logger.info(f"Needs attention (low confidence={book_confidence}): {old_path.name}")
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                 ('needs_attention', f'Low confidence ({book_confidence}) - could not verify identification', row['book_id']))
                conn.commit()
                processed += 1
                continue

            # Issue #59: If author is placeholder (Unknown, etc.), advance to Layer 3 for audio analysis
            if is_placeholder_author(new_author):
                # Check if audio analysis is enabled before advancing
                if config.get('enable_audio_analysis', False):
                    logger.info(f"Advancing to Layer 3 (AI returned placeholder '{new_author}'): {row['current_author']}/{row['current_title']}")
                    c.execute('UPDATE books SET verification_layer = 3 WHERE id = ?', (row['book_id'],))
                    conn.commit()
                    processed += 1
                    continue
                else:
                    # Audio analysis disabled - mark as needs_attention
                    logger.info(f"NEEDS ATTENTION (placeholder author '{new_author}', no audio analysis): {row['current_author']}/{row['current_title']}")
                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                                      new_narrator, new_series, new_series_num, new_year, new_edition, new_variant)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'needs_attention', ?, ?, ?, ?, ?, ?, ?)''',
                             (row['book_id'], row['current_author'], row['current_title'],
                              new_author, new_title, str(old_path), str(new_path),
                              f"Could not identify author (got '{new_author}')",
                              new_narrator, new_series, str(new_series_num) if new_series_num else None,
                              str(new_year) if new_year else None, new_edition, new_variant))
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('needs_attention', f"Could not identify author (got '{new_author}')", row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    processed += 1
                    continue

            # Only auto-fix if enabled AND NOT a drastic change (unless Trust the Process mode)
            # In Trust the Process mode, verified drastic changes can be auto-fixed
            trust_mode = config.get('trust_the_process', False)
            can_auto_fix = config.get('auto_fix', False) and (not drastic_change or trust_mode)
            if can_auto_fix:
                # Actually rename the folder
                try:
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
                                    language_code=lang_code,
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
                                        language_code=lang_code,
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
                                            language_code=lang_code,
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
                    # Issue #79: Also prevent duplicate 'fixed' history entries by deleting existing ones
                    c.execute("DELETE FROM history WHERE book_id = ? AND status IN ('pending_fix', 'fixed')", (row['book_id'],))

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
                # Validate before creating pending_fix (Issue #92: prevent garbage recommendations)
                if not is_valid_author_for_recommendation(new_author):
                    logger.warning(f"[LAYER 2] Rejected garbage author: '{new_author}' for {row['current_title']}")
                    c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    continue
                if not is_valid_title_for_recommendation(new_title):
                    logger.warning(f"[LAYER 2] Rejected garbage title: '{new_title}' for {row['current_author']}")
                    c.execute('UPDATE books SET status = ? WHERE id = ?', ('needs_attention', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    continue
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
            # No fix needed - AI confirmed current values are correct
            # Issue #59: check if author is placeholder
            if is_placeholder_author(row['current_author']):
                c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                         ('needs_attention', f"Could not identify author (currently '{row['current_author']}')", row['book_id']))
                logger.info(f"Needs attention (placeholder author): {row['current_author']}/{row['current_title']}")
            else:
                # Create profile documenting that AI verified this book
                profile = BookProfile()
                profile.add_author('ai', new_author)
                profile.add_title('ai', new_title)
                if new_series:
                    profile.series.add_source('ai', new_series)
                if new_series_num:
                    profile.series_num.add_source('ai', new_series_num)
                if new_narrator:
                    profile.narrator.add_source('ai', new_narrator)
                profile.verification_layers_used = ['ai']
                profile.finalize()

                c.execute('UPDATE books SET status = ?, profile = ?, confidence = ? WHERE id = ?',
                         ('verified', json.dumps(profile.to_dict()), profile.overall_confidence, row['book_id']))
                logger.info(f"Verified OK (AI confirmed): {row['current_author']}/{row['current_title']} (conf={profile.overall_confidence})")

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


__all__ = ['process_queue']
