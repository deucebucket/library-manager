"""Layer 1: Audio Transcription + AI Parsing

Philosophy: Audio is the source of truth. Narrators announce the book.
1. Transcribe first 90 seconds (the intro)
2. AI extracts: author, title, narrator, series
3. High confidence → identified
4. Low confidence → advance to Layer 2

This replaces the old "trust folder names" approach.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


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
        identify_audio_with_bookdb: Function to identify audio via BookDB API
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

        # Find first audio file
        audio_file = None
        for ext in ['.m4b', '.mp3', '.m4a', '.flac', '.ogg']:
            files = list(Path(book_path).glob(f'*{ext}'))
            if files:
                audio_file = files[0]
                break

        if not audio_file:
            # No audio file - this is likely an ebook. Try to identify from filename + BookDB
            filename = os.path.basename(book_path)
            logger.debug(f"[EBOOK] No audio, trying filename + BookDB for: {filename}")
            ebook_result = identify_ebook_from_filename(filename, book_path, config)

            if ebook_result and ebook_result.get('author') and ebook_result.get('title'):
                # Got identification from filename + BookDB!
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
                        computed_path = build_new_path(
                            Path(library_paths[0]), author, title,
                            series=ebook_result.get('series'),
                            series_num=ebook_result.get('series_num'),
                            config=current_config
                        )
                        if computed_path:
                            new_path_str = str(computed_path)

                    # Add to history as pending fix (with paths to prevent stale references)
                    c.execute('''INSERT INTO history
                                (book_id, old_author, old_title, new_author, new_title, new_series, new_series_num, old_path, new_path, status)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_fix')''',
                             (row['book_id'], row['current_author'], row['current_title'],
                              author, title, ebook_result.get('series'), ebook_result.get('series_num'),
                              old_path_str, new_path_str))

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

        # === TRY BOOKDB API FIRST (GPU Whisper + 50M book database) ===
        # This avoids Gemini rate limits and is faster
        result = None
        transcript = None

        if config.get('use_bookdb_for_audio', True):
            # Issue #74: Check if BookDB circuit breaker is open - wait instead of skipping
            if is_circuit_open('bookdb'):
                cb = get_circuit_breaker('bookdb')
                remaining = int(cb.get('circuit_open_until', 0) - time.time())
                if remaining > 0:
                    wait_time = min(remaining, 60)  # Wait up to 60s at a time
                    logger.info(f"[LAYER 1/AUDIO] BookDB circuit breaker open, waiting {wait_time}s ({remaining}s total remaining)")
                    if update_processing_status:
                        update_processing_status(f"Layer 1: Waiting for BookDB ({remaining}s)")
                    time.sleep(wait_time)
                    # After waiting, continue to next item - circuit breaker may have closed
                    processed += 1
                    continue

            result = identify_audio_with_bookdb(audio_file)
            if result and result.get('author') and result.get('title'):
                # BookDB got a full identification - use it!
                logger.info(f"[LAYER 1/AUDIO] BookDB identified: {result['author']} - {result['title']}")
            else:
                # BookDB didn't get a full match - might have a transcript though
                transcript = result.get('transcript') if result else None
                result = None  # Clear partial result to trigger AI fallback
        else:
            logger.info(f"[LAYER 1/AUDIO] BookDB audio disabled, using local transcription + AI")

        # If no result yet, try local transcription + AI
        if not result:
            if not transcript:
                # No transcript from BookDB (or BookDB disabled), do local transcription
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

            # Parse with AI (fallback path - when BookDB disabled or didn't identify)
            result = parse_transcript_with_ai(transcript, folder_hint, config)

        if result and result.get('author') and result.get('title') and result.get('confidence') != 'none':
            # Got identification from audio!
            author = result.get('author')
            title = result.get('title')
            narrator = result.get('narrator')
            series = result.get('series')
            series_num = result.get('series_num')
            confidence = result.get('confidence', 'medium')

            logger.info(f"[LAYER 1/AUDIO] Identified from audio: {author} - {title} ({confidence})")

            # Check if different from current (handle None values)
            current_author = row['current_author'] or ''
            current_title = row['current_title'] or ''
            if author.lower() != current_author.lower() or title.lower() != current_title.lower():
                # Needs fix - will be handled by existing fix mechanism
                conn = get_db()
                c = conn.cursor()

                # Update book with audio-identified info
                profile = {
                    'author': {'value': author, 'source': 'audio_transcription', 'confidence': 85 if confidence == 'high' else 70},
                    'title': {'value': title, 'source': 'audio_transcription', 'confidence': 85 if confidence == 'high' else 70},
                }
                if narrator:
                    profile['narrator'] = {'value': narrator, 'source': 'audio_transcription', 'confidence': 80}
                if series:
                    profile['series'] = {'value': series, 'source': 'audio_transcription', 'confidence': 75}
                if series_num:
                    profile['series_num'] = {'value': str(series_num), 'source': 'audio_transcription', 'confidence': 75}

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
                    computed_path = build_new_path(
                        Path(library_paths[0]), author, title,
                        series=series, series_num=series_num, narrator=narrator,
                        config=audio_config
                    )
                    if computed_path:
                        new_path_str = str(computed_path)

                # Add to history (with paths to prevent stale references)
                c.execute('''INSERT INTO history
                            (book_id, old_author, old_title, new_author, new_title, new_narrator, new_series, new_series_num, old_path, new_path, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_fix')''',
                         (row['book_id'], row['current_author'], row['current_title'],
                          author, title, narrator, series, series_num,
                          old_path_str, new_path_str))

                # Remove from queue
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                conn.commit()
                conn.close()

                resolved += 1
            else:
                # Already correct - but update confidence since we did identify via audio
                audio_confidence = 85 if confidence == 'high' else 70
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
