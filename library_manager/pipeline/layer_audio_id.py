"""Layer 1: Audio Transcription + AI Parsing + Voice ID

Philosophy: Audio is the source of truth. Narrators announce the book.
1. Transcribe first 90 seconds (the intro)
2. AI extracts: author, title, narrator, series
3. Voice fingerprint identifies narrator even when not mentioned
4. High confidence → identified
5. Low confidence → advance to Layer 2

This replaces the old "trust folder names" approach.
Part of the Skaldleita voice ID system - "Shazam for audiobook narrators"
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from library_manager.config import use_skaldleita_for_audio
from library_manager.database import insert_history_entry
from library_manager.utils.validation import (
    is_garbage_author_match, is_placeholder_author,
    is_valid_author_for_recommendation, is_valid_title_for_recommendation
)
from library_manager.worker import set_current_provider, set_api_latency, set_confidence

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

logger = logging.getLogger(__name__)

# Voice ID integration - lazy imported to avoid startup penalty
_voice_id_available = None


def _check_voice_id() -> bool:
    """Check if voice ID is available (pyannote installed)."""
    global _voice_id_available
    if _voice_id_available is None:
        try:
            from library_manager.providers.fingerprint import extract_voice_embedding
            _voice_id_available = True
            logger.info("[VOICE] Voice ID module available")
        except ImportError:
            _voice_id_available = False
            logger.debug("[VOICE] Voice ID not available (pyannote not installed)")
    return _voice_id_available


def _store_voice_and_identify_narrator(
    audio_path: str,
    result: Dict,
    config: Dict
) -> Optional[str]:
    """
    Store voice signature and try to identify narrator by voice.

    When use_skaldleita_for_audio=true (default), Skaldleita handles voice ID
    server-side, so this function does nothing. Local voice ID via pyannote
    is only used when the user explicitly disables Skaldleita audio.

    Returns:
        Narrator name if identified by voice, None otherwise
    """
    # When Skaldleita handles audio, voice ID is done server-side
    # No need for local pyannote - skip entirely
    if use_skaldleita_for_audio(config):
        return None

    if not _check_voice_id():
        return None

    try:
        from library_manager.providers.fingerprint import (
            store_voice_after_identification,
            identify_narrator_by_voice
        )

        api_key = config.get('bookdb_api_key', '')

        # First, try to identify narrator by voice alone
        identified_narrator = None
        if not result.get('narrator'):
            identified_narrator = identify_narrator_by_voice(
                str(audio_path),
                threshold=0.6,
                api_key=api_key
            )
            if identified_narrator:
                logger.info(f"[VOICE] Identified narrator by voice: {identified_narrator}")

        # Always store the voice signature for future matching
        # Include narrator from either transcript or voice match
        store_result = result.copy()
        if identified_narrator and not store_result.get('narrator'):
            store_result['narrator'] = identified_narrator

        stored = store_voice_after_identification(
            str(audio_path),
            store_result,
            api_key=api_key
        )

        if stored:
            logger.debug(f"[VOICE] Stored voice signature for: {result.get('title', 'Unknown')}")

        return identified_narrator

    except Exception as e:
        logger.warning(f"[VOICE] Voice ID error (non-fatal): {e}")
        return None


def _validate_ai_result_against_path(result: Dict, folder_hint: str, book_path: str) -> Dict:
    """
    Sanity check: Compare AI result against path/folder information.

    If the AI returned something completely different from what the path suggests,
    reduce confidence. This catches cases like:
    - Path: "ANNIE JACOBSEN/Operation Paperclip"
    - AI: Author="And Charles R", Title="Allen, Jr"  (completely wrong)

    Returns the result with potentially modified confidence.
    """
    if not result or not folder_hint:
        return result

    ai_author = (result.get('author') or '').lower()
    ai_title = (result.get('title') or '').lower()

    # Extract path components for comparison
    path_parts = book_path.lower() if book_path else folder_hint.lower()
    hint_parts = folder_hint.lower()

    # Clean up - remove common noise
    def clean_text(text):
        import re
        # Remove brackets, hashes, special chars
        text = re.sub(r'\[[^\]]*\]', ' ', text)
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        return set(text.split())

    path_words = clean_text(path_parts)
    hint_words = clean_text(hint_parts)
    ai_author_words = clean_text(ai_author)
    ai_title_words = clean_text(ai_title)

    # Check if ANY significant word from AI matches path/hint
    significant_ai_words = {w for w in (ai_author_words | ai_title_words) if len(w) > 3}
    path_hint_words = {w for w in (path_words | hint_words) if len(w) > 3}

    overlap = significant_ai_words & path_hint_words

    # If no overlap at all and we have significant words from both, this is suspicious
    if len(significant_ai_words) >= 2 and len(path_hint_words) >= 2 and not overlap:
        logger.warning(f"[SANITY CHECK] AI result has NO overlap with path info!")
        logger.warning(f"  AI: {result.get('author')} - {result.get('title')}")
        logger.warning(f"  Path hint: {folder_hint}")

        # Severely reduce confidence - this is likely a misparse
        result['confidence'] = 'none'
        result['sanity_failed'] = True
        result['sanity_reason'] = 'AI result completely mismatched path info'
        return result

    # If only partial overlap (less than 50% of AI words match), reduce confidence
    if len(significant_ai_words) > 0:
        match_ratio = len(overlap) / len(significant_ai_words)
        if match_ratio < 0.3:  # Less than 30% match
            logger.info(f"[SANITY CHECK] Low overlap ({match_ratio:.0%}) - reducing confidence")
            if result.get('confidence') == 'high':
                result['confidence'] = 'medium'
            elif result.get('confidence') == 'medium':
                result['confidence'] = 'low'

    return result


def process_layer_1_audio(
    config: Dict,
    get_db: Callable,
    identify_ebook_from_filename: Callable,
    identify_audio_with_bookdb: Callable,
    transcribe_audio_intro: Callable,
    parse_transcript_with_ai: Callable,
    is_circuit_open: Callable,
    get_circuit_breaker: Callable,
    load_config: Callable,
    build_new_path: Callable,
    update_processing_status: Optional[Callable] = None,
    set_current_book: Optional[Callable] = None,
    limit: Optional[int] = None
) -> Tuple[int, int]:
    """
    Layer 1: Audio Transcription + AI Parsing

    Processes items at verification_layer 0/1 using audio transcription.
    Items that get a confident match are marked complete.
    Items that fail are advanced to Layer 2.

    Args:
        config: App configuration dict
        get_db: Function to get database connection
        identify_ebook_from_filename: Function to identify ebooks from filename
        identify_audio_with_bookdb: Function to identify audio via Skaldleita API
        transcribe_audio_intro: Function to transcribe audio locally
        parse_transcript_with_ai: Function to parse transcript with AI
        is_circuit_open: Function to check if circuit breaker is open
        get_circuit_breaker: Function to get circuit breaker state dict
        load_config: Function to load config (for path building)
        build_new_path: Function to build new paths
        update_processing_status: Optional function to update processing status
        limit: Maximum batch size (overrides config)

    Returns:
        Tuple of (processed_count, resolved_count)
    """
    batch_size = limit or config.get('batch_size', 3)

    # Get items from queue
    conn = get_db()
    c = conn.cursor()

    # Process items that haven't been through audio identification yet
    # ALSO include 'needs_attention' items - they failed old system, might succeed with audio
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title, b.verification_layer
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.verification_layer IN (0, 1)
                   AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files')
                   AND (b.user_locked IS NULL OR b.user_locked = 0)
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (batch_size,))
    batch = [dict(row) for row in c.fetchall()]
    conn.close()

    if not batch:
        return 0, 0

    logger.info(f"[LAYER 1/AUDIO] Processing {len(batch)} items via audio transcription")

    processed = 0
    resolved = 0

    for row in batch:
        book_path = row['path']
        folder_hint = f"{row['current_author']} - {row['current_title']}"

        # Update status bar with current book
        if set_current_book:
            set_current_book(
                row['current_author'] or 'Unknown',
                row['current_title'] or 'Unknown',
                "Identifying via audio intro..."
            )

        # Find first audio file
        audio_file = None
        for ext in ['.m4b', '.mp3', '.m4a', '.flac', '.ogg']:
            files = list(Path(book_path).glob(f'*{ext}'))
            if files:
                audio_file = files[0]
                break

        if not audio_file:
            # No audio file - this is likely an ebook. Try to identify from filename + Skaldleita
            filename = os.path.basename(book_path)
            logger.debug(f"[EBOOK] No audio, trying filename + Skaldleita for: {filename}")
            ebook_result = identify_ebook_from_filename(filename, book_path, config)

            if ebook_result and ebook_result.get('author') and ebook_result.get('title'):
                # Got identification from filename + Skaldleita!
                author = ebook_result.get('author')
                title = ebook_result.get('title')
                confidence = ebook_result.get('confidence', 'medium')
                source = ebook_result.get('source', 'bookdb')

                logger.info(f"[EBOOK] Identified via {source}: {author} - {title} ({confidence})")

                # Check if different from current
                current_author = row['current_author'] or ''
                current_title = row['current_title'] or ''
                if author.lower() != current_author.lower() or title.lower() != current_title.lower():
                    conn = get_db()
                    c = conn.cursor()

                    # Update book with ebook-identified info
                    profile = {
                        'author': {'value': author, 'source': source, 'confidence': 80 if confidence == 'high' else 50},
                        'title': {'value': title, 'source': source, 'confidence': 80 if confidence == 'high' else 50},
                    }
                    if ebook_result.get('series'):
                        profile['series'] = {'value': ebook_result['series'], 'source': source, 'confidence': 70}
                    if ebook_result.get('series_num'):
                        profile['series_num'] = {'value': ebook_result['series_num'], 'source': source, 'confidence': 70}

                    c.execute('''UPDATE books SET status = 'pending',
                                profile = ?, confidence = ?, verification_layer = 2
                                WHERE id = ?''',
                              (json.dumps(profile), 80 if confidence == 'high' else 50, row['book_id']))

                    # Compute paths for history entry (Issue #64: prevent stale path errors)
                    old_path_str = book_path
                    current_config = load_config()
                    library_paths = current_config.get('library_paths', [])
                    new_path_str = None
                    if library_paths:
                        # Issue #135: Use output folder for watch folder items
                        dest_path = Path(library_paths[0])
                        watch_folder = current_config.get('watch_folder', '').strip()
                        watch_output = current_config.get('watch_output_folder', '').strip()
                        if watch_folder and watch_output:
                            try:
                                if Path(book_path).resolve().is_relative_to(Path(watch_folder).resolve()):
                                    dest_path = Path(watch_output)
                            except Exception:
                                pass
                        # Detect language for multi-language naming
                        lang_code = _detect_title_language(title)
                        computed_path = build_new_path(
                            dest_path, author, title,
                            series=ebook_result.get('series'),
                            series_num=ebook_result.get('series_num'),
                            language_code=lang_code,
                            config=current_config
                        )
                        if computed_path:
                            new_path_str = str(computed_path)

                    # Validate before creating pending_fix (reject garbage recommendations)
                    if not is_valid_author_for_recommendation(author):
                        logger.warning(f"[LAYER 1] Rejected garbage author: '{author}' for {row['current_title']}")
                        conn.close()
                        continue
                    if not is_valid_title_for_recommendation(title):
                        logger.warning(f"[LAYER 1] Rejected garbage title: '{title}' for {row['current_author']}")
                        conn.close()
                        continue

                    # Add to history as pending fix (with paths to prevent stale references)
                    # Issue #79: Use helper function to prevent duplicates
                    insert_history_entry(
                        c, row['book_id'], row['current_author'], row['current_title'],
                        author, title, old_path_str, new_path_str, 'pending_fix',
                        new_series=ebook_result.get('series'),
                        new_series_num=ebook_result.get('series_num')
                    )

                    # Remove from queue
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    conn.commit()
                    conn.close()
                    resolved += 1
                else:
                    # Only mark as verified if we have confidence in the identification
                    book_confidence = row.get('confidence', 0) or 0
                    conn = get_db()
                    c = conn.cursor()
                    if book_confidence >= 40:
                        logger.info(f"[EBOOK] Already correct (conf={book_confidence}): {current_author}/{current_title}")
                        c.execute('UPDATE books SET status = ?, verification_layer = 2 WHERE id = ?',
                                  ('verified', row['book_id']))
                        resolved += 1
                    else:
                        logger.info(f"[EBOOK] Needs attention (low conf={book_confidence}): {current_author}/{current_title}")
                        c.execute('UPDATE books SET status = ?, verification_layer = 2, error_message = ? WHERE id = ?',
                                  ('needs_attention', f'Low confidence ({book_confidence}) - ebook needs manual verification', row['book_id']))
                    conn.commit()
                    conn.close()

                processed += 1
                continue

            # Ebook identification failed - advance to Layer 2 for API lookups
            logger.debug(f"[EBOOK] Filename parsing failed, advancing to Layer 2: {book_path}")
            conn = get_db()
            c = conn.cursor()
            c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
            conn.commit()
            conn.close()
            processed += 1
            continue

        # === TRY SKALDLEITA API FIRST (GPU Whisper + 50M book database) ===
        # This avoids Gemini rate limits and is faster
        result = None
        transcript = None

        if use_skaldleita_for_audio(config):
            # Issue #74: Check if Skaldleita circuit breaker is open - wait instead of skipping
            if is_circuit_open('bookdb'):
                cb = get_circuit_breaker('bookdb')
                remaining = int(cb.get('circuit_open_until', 0) - time.time())
                if remaining > 0:
                    wait_time = min(remaining, 60)  # Wait up to 60s at a time
                    logger.info(f"[LAYER 1/AUDIO] Skaldleita circuit breaker open, waiting {wait_time}s ({remaining}s total remaining)")
                    set_current_provider("Skaldleita", f"Circuit breaker open ({remaining}s)", is_free=True)
                    if update_processing_status:
                        update_processing_status(f"Layer 1: Waiting for Skaldleita ({remaining}s)")
                    time.sleep(wait_time)
                    # After waiting, continue to next item - circuit breaker may have closed
                    processed += 1
                    continue

            # Show status: Using Skaldleita (free, GPU Whisper)
            set_current_provider("Skaldleita", "Transcribing audio with GPU Whisper...", is_free=True)
            api_start = time.time()
            bookdb_result = identify_audio_with_bookdb(audio_file)
            set_api_latency(int((time.time() - api_start) * 1000))

            # Phase 5: Handle requeue_suggested from SL (Skaldleita backbone)
            # If SL has author/title but suggests requeue (live scrape added to staging),
            # create a pending_fix NOW but schedule recheck for tomorrow
            if bookdb_result and bookdb_result.get('requeue_suggested', False):
                sl_source = bookdb_result.get('sl_source', 'unknown')
                sl_author = bookdb_result.get('author')
                sl_title = bookdb_result.get('title')

                if sl_author and sl_title:
                    # SL found author/title - trust it but schedule requeue for nightly merge
                    logger.info(f"[LAYER 1/AUDIO] SL requeue with partial ID (source: {sl_source}): {sl_author} - {sl_title}")

                    # Store as pending_fix - this is trusted SL identification
                    result = {
                        'author': sl_author,
                        'title': sl_title,
                        'narrator': bookdb_result.get('narrator'),
                        'series': bookdb_result.get('series'),
                        'series_num': bookdb_result.get('series_num'),
                        'confidence': bookdb_result.get('confidence', 0.75),
                        'sl_source': sl_source,
                        'requeue_suggested': True
                    }
                    # Continue processing - let the normal flow create pending_fix
                    # The requeue flag will be used to schedule a future recheck
                else:
                    # No author/title - just advance to layer 2 for AI
                    logger.info(f"[LAYER 1/AUDIO] SL requeue no ID (source: {sl_source}) - trying AI: {book_path}")
                    conn = get_db()
                    c = conn.cursor()
                    c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
                    conn.commit()
                    conn.close()
                    processed += 1
                    continue
            elif bookdb_result and bookdb_result.get('author') and bookdb_result.get('title'):
                # Skaldleita got a full identification - validate against path first
                sl_source = bookdb_result.get('sl_source', 'audio')
                # Safely parse confidence - Skaldleita may return float, string, or garbage
                raw_confidence = bookdb_result.get('confidence', 0.8)
                try:
                    sl_confidence = int(float(raw_confidence) * 100) if isinstance(raw_confidence, (int, float)) else 80
                except (ValueError, TypeError):
                    sl_confidence = 80  # Default if parsing fails
                set_current_provider("Skaldleita", f"Identified from {sl_source}", is_free=True)
                set_confidence(sl_confidence)
                logger.info(f"[LAYER 1/AUDIO] Skaldleita identified (source: {sl_source}): {bookdb_result['author']} - {bookdb_result['title']}")
                # Sanity check: validate against path info to catch misparses
                bookdb_result = _validate_ai_result_against_path(bookdb_result, folder_hint, book_path)
                if bookdb_result.get('sanity_failed'):
                    logger.warning(f"[LAYER 1/AUDIO] Skaldleita result failed sanity check - will try AI fallback")
                    transcript = bookdb_result.get('transcript')  # Keep transcript for AI
                    result = None  # Clear to trigger AI fallback
                else:
                    result = bookdb_result  # Passed sanity check
            else:
                # Skaldleita didn't get a full match - might have a transcript though
                transcript = bookdb_result.get('transcript') if bookdb_result else None
                result = None  # Clear partial result to trigger AI fallback
        else:
            logger.info(f"[LAYER 1/AUDIO] Skaldleita audio disabled, using local transcription + AI")
            set_current_provider("Local Whisper", "Transcribing audio locally...", is_free=True)

        # If no result yet, try local transcription + AI
        if not result:
            if not transcript:
                # No transcript from Skaldleita (or Skaldleita disabled), do local transcription
                set_current_provider("Local Whisper", "Transcribing audio locally...", is_free=True)
                transcript = transcribe_audio_intro(audio_file)

            if not transcript:
                logger.warning(f"[LAYER 1/AUDIO] Transcription failed, advancing to Layer 2: {book_path}")
                conn = get_db()
                c = conn.cursor()
                # Reset status to pending if it was needs_attention - we still have more layers to try
                c.execute('''UPDATE books SET verification_layer = 2,
                            status = CASE WHEN status = 'needs_attention' THEN 'pending' ELSE status END
                            WHERE id = ?''', (row['book_id'],))
                conn.commit()
                conn.close()
                processed += 1
                continue

            # Parse with AI (fallback path - when Skaldleita disabled or didn't identify)
            ai_provider = config.get('ai_provider', 'gemini')
            set_current_provider(ai_provider.title(), "Parsing transcript with AI...", is_free=(ai_provider == 'ollama'))
            result = parse_transcript_with_ai(transcript, folder_hint, config)

            # Sanity check: validate AI result against path info
            # This catches cases where AI completely misparses (e.g., narrator name as author)
            if result:
                result = _validate_ai_result_against_path(result, folder_hint, book_path)

        if result and result.get('author') and result.get('title') and result.get('confidence') != 'none':
            # Got identification from audio!
            author = result.get('author')
            title = result.get('title')
            narrator = result.get('narrator')
            series = result.get('series')
            series_num = result.get('series_num')
            confidence = result.get('confidence', 'medium')

            logger.info(f"[LAYER 1/AUDIO] Identified from audio: {author} - {title} ({confidence})")

            # === SKALDLEITA VOICE ID ===
            # Store voice signature and try to identify narrator by voice
            # This builds the narrator voice library and fills in missing narrator info
            if audio_file and config.get('enable_voice_id', True):
                voice_narrator = _store_voice_and_identify_narrator(
                    str(audio_file), result, config
                )
                if voice_narrator and not narrator:
                    narrator = voice_narrator
                    result['narrator'] = narrator
                    logger.info(f"[LAYER 1/AUDIO] Narrator identified by voice: {narrator}")

            # Check if different from current (handle None values)
            current_author = row['current_author'] or ''
            current_title = row['current_title'] or ''
            if author.lower() != current_author.lower() or title.lower() != current_title.lower():
                # Validate that the extracted author isn't garbage
                # "earth" from "Middle-earth" or single words shouldn't replace real authors
                author_is_garbage = False
                if author and len(author) < 4:
                    # Too short to be a real author name
                    author_is_garbage = True
                    logger.info(f"[LAYER 1/AUDIO] Rejecting garbage author (too short): '{author}'")
                elif current_author and not is_placeholder_author(current_author):
                    # Current author is real - check if new author is garbage match
                    if is_garbage_author_match(current_author, author):
                        author_is_garbage = True
                        logger.info(f"[LAYER 1/AUDIO] Rejecting garbage author match: '{current_author}' -> '{author}'")

                if author_is_garbage:
                    # Don't create pending fix with garbage author - advance to layer 2
                    conn = get_db()
                    c = conn.cursor()
                    c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (row['book_id'],))
                    conn.commit()
                    conn.close()
                    logger.info(f"[LAYER 1/AUDIO] Garbage author rejected, advancing to Layer 2: {current_author}/{current_title}")
                    processed += 1
                    continue

                # Needs fix - will be handled by existing fix mechanism
                conn = get_db()
                c = conn.cursor()

                # Update book with audio-identified info
                sl_source = result.get('sl_source', 'audio_transcription')
                base_confidence = 85 if confidence == 'high' else 70
                profile = {
                    'author': {'value': author, 'source': sl_source, 'confidence': base_confidence},
                    'title': {'value': title, 'source': sl_source, 'confidence': base_confidence},
                }
                if narrator:
                    profile['narrator'] = {'value': narrator, 'source': sl_source, 'confidence': 80}
                if series:
                    profile['series'] = {'value': series, 'source': sl_source, 'confidence': 75}
                if series_num:
                    profile['series_num'] = {'value': str(series_num), 'source': sl_source, 'confidence': 75}

                # Phase 5: Track SL requeue suggestion for future re-verification
                # After nightly merge, the book should be re-checked against main DB
                if result.get('requeue_suggested'):
                    # Schedule requeue for tomorrow at 6am (after nightly merge at 4am)
                    tomorrow_6am = (datetime.now() + timedelta(days=1)).replace(hour=6, minute=0, second=0)
                    profile['sl_requeue'] = {
                        'suggested_at': datetime.now().isoformat(),
                        'requeue_after': tomorrow_6am.isoformat(),
                        'reason': 'SL live scrape - pending nightly merge'
                    }
                    logger.info(f"[LAYER 1/AUDIO] Scheduled SL requeue for {tomorrow_6am.strftime('%Y-%m-%d %H:%M')}: {author} - {title}")

                c.execute('''UPDATE books SET
                            current_author = ?, current_title = ?,
                            status = 'pending_fix', verification_layer = 3,
                            profile = ?, confidence = ?
                            WHERE id = ?''',
                         (author, title, json.dumps(profile),
                          85 if confidence == 'high' else 70, row['book_id']))

                # Compute paths for history entry (Issue #64: prevent stale path errors)
                old_path_str = book_path
                audio_config = load_config()
                library_paths = audio_config.get('library_paths', [])
                new_path_str = None
                if library_paths:
                    # Issue #135: Use output folder for watch folder items
                    dest_path = Path(library_paths[0])
                    watch_folder = audio_config.get('watch_folder', '').strip()
                    watch_output = audio_config.get('watch_output_folder', '').strip()
                    if watch_folder and watch_output:
                        try:
                            if Path(book_path).resolve().is_relative_to(Path(watch_folder).resolve()):
                                dest_path = Path(watch_output)
                        except Exception:
                            pass
                    # Detect language for multi-language naming
                    lang_code = _detect_title_language(title)
                    computed_path = build_new_path(
                        dest_path, author, title,
                        series=series, series_num=series_num, narrator=narrator,
                        language_code=lang_code,
                        config=audio_config
                    )
                    if computed_path:
                        new_path_str = str(computed_path)

                # Validate before creating pending_fix (Issue #92: prevent garbage recommendations)
                if not is_valid_author_for_recommendation(author):
                    logger.warning(f"[LAYER 1/AUDIO] Rejected garbage author: '{author}' for {row['current_title']}")
                    # Don't create garbage pending_fix - advance to Layer 2 instead
                    c.execute('''UPDATE books SET verification_layer = 2,
                                status = CASE WHEN status = 'needs_attention' THEN 'pending' ELSE status END
                                WHERE id = ?''', (row['book_id'],))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    conn.commit()
                    conn.close()
                    continue

                if not is_valid_title_for_recommendation(title):
                    logger.warning(f"[LAYER 1/AUDIO] Rejected garbage title: '{title}' for {row['current_author']}")
                    # Don't create garbage pending_fix - advance to Layer 2 instead
                    c.execute('''UPDATE books SET verification_layer = 2,
                                status = CASE WHEN status = 'needs_attention' THEN 'pending' ELSE status END
                                WHERE id = ?''', (row['book_id'],))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    conn.commit()
                    conn.close()
                    continue

                # Add to history (with paths to prevent stale references)
                # Issue #79: Use helper function to prevent duplicates
                insert_history_entry(
                    c, row['book_id'], row['current_author'], row['current_title'],
                    author, title, old_path_str, new_path_str, 'pending_fix',
                    new_narrator=narrator, new_series=series, new_series_num=series_num
                )

                # Remove from queue
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                conn.commit()
                conn.close()

                resolved += 1
            else:
                # Already correct - but update confidence since we did identify via audio
                audio_confidence = 85 if confidence == 'high' else 70

                # Store voice signature for correctly-named books too
                # This builds the narrator library from verified audiobooks
                if audio_file and config.get('enable_voice_id', True):
                    _store_voice_and_identify_narrator(str(audio_file), result, config)

                conn = get_db()
                c = conn.cursor()
                c.execute('UPDATE books SET status = ?, verification_layer = 3, confidence = ? WHERE id = ?',
                         ('verified', audio_confidence, row['book_id']))
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                conn.commit()
                conn.close()

                logger.info(f"[LAYER 1/AUDIO] Already correct (conf={audio_confidence}): {author}/{title}")
                resolved += 1
        else:
            # Couldn't identify from transcript, advance to Layer 2
            logger.info(f"[LAYER 1/AUDIO] Unclear transcript, advancing to Layer 2: {folder_hint}")
            conn = get_db()
            c = conn.cursor()
            # Reset status to pending if it was needs_attention - we still have more layers to try
            c.execute('''UPDATE books SET verification_layer = 2,
                        status = CASE WHEN status = 'needs_attention' THEN 'pending' ELSE status END
                        WHERE id = ?''', (row['book_id'],))
            conn.commit()
            conn.close()

        processed += 1

    logger.info(f"[LAYER 1/AUDIO] Processed {processed}, resolved {resolved} via audio transcription")
    return processed, resolved


__all__ = ['process_layer_1_audio']
