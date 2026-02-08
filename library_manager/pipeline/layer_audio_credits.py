"""Layer 3: Audio Credits Analysis

Processes items using Gemini audio analysis to extract metadata from
audiobook intros (title/author/narrator announcements).

This is an expensive layer that makes external API calls.
"""

import logging
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from library_manager.config import load_secrets
from library_manager.database import insert_history_entry
from library_manager.providers import is_circuit_open, API_CIRCUIT_BREAKER
from library_manager.utils.naming import calculate_title_similarity
from library_manager.utils.path_safety import build_new_path

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


def process_layer_3_audio(
    config: Dict,
    get_db: Callable,
    find_audio_files: Callable,
    analyze_audio_for_credits: Callable,
    auto_save_narrator: Optional[Callable] = None,
    contribute_audio_extraction: Optional[Callable] = None,
    standardize_initials: Optional[Callable] = None,
    limit: Optional[int] = None,
    verification_layer: int = 3
) -> Tuple[int, int]:
    """
    Layer 2/3: Audio Analysis

    Processes items at specified verification_layer using Gemini audio analysis.
    This is an expensive layer - extracts metadata from audiobook intros.
    Items that still can't be identified are marked as 'needs_attention'.

    Args:
        config: App configuration dict
        get_db: Function to get database connection
        find_audio_files: Function to find audio files in a directory
        analyze_audio_for_credits: Function to analyze audio for credits
        auto_save_narrator: Optional function to save narrator info
        contribute_audio_extraction: Optional function for community contribution
        standardize_initials: Optional function to standardize author initials
        limit: Maximum batch size (overrides config)
        verification_layer: Which layer to process (2 for unclear L1 results, 3 for API failures)

    Returns:
        Tuple of (processed_count, resolved_count)

    NOTE: This function uses a 3-phase approach to avoid holding DB locks during
    expensive Gemini audio analysis calls (which can take 10-30+ seconds per item):
      Phase 1: Quick fetch, release connection
      Phase 2: Audio analysis (no DB connection held)
      Phase 3: Quick write, release connection
    """
    if not config.get('enable_audio_analysis', False):
        logger.info("[LAYER 3] Audio analysis disabled, skipping")
        return 0, 0

    # Check if we have Gemini API key
    secrets = load_secrets()
    if not secrets or not secrets.get('gemini_api_key'):
        logger.info("[LAYER 3] No Gemini API key for audio analysis, skipping")
        return 0, 0

    # Check Gemini circuit breaker - don't process if Gemini is unavailable
    if is_circuit_open('gemini'):
        cb = API_CIRCUIT_BREAKER.get('gemini', {})
        remaining = int(cb.get('circuit_open_until', 0) - time.time())
        logger.info(f"[LAYER 3] Gemini circuit breaker open ({remaining}s remaining), pausing audio analysis")
        return 0, 0  # Return 0,0 to signal caller to wait

    # === PHASE 1: Fetch batch (quick read, release connection immediately) ===
    conn = get_db()
    c = conn.cursor()

    batch_size = limit or config.get('batch_size', 3)

    # Get items awaiting audio analysis at specified verification layer
    # Skip user-locked books - user has manually set metadata
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.verification_layer = ?
                   AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                   AND (b.user_locked IS NULL OR b.user_locked = 0)
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (verification_layer, batch_size,))
    # Convert to dicts immediately - sqlite3.Row objects become invalid after conn.close()
    batch = [dict(row) for row in c.fetchall()]
    conn.close()  # Release DB lock BEFORE expensive audio analysis

    if not batch:
        return 0, 0

    logger.info(f"[LAYER 3] Processing {len(batch)} items via audio analysis")

    # === PHASE 2: Audio analysis (NO database connection held) ===
    # Collect all analysis results, then apply them in phase 3
    analysis_results = []  # List of (row, action_type, action_data)

    for row in batch:
        path = row['path']
        book_path = Path(path)

        # Find audio files in this folder
        audio_files = find_audio_files(str(book_path)) if book_path.is_dir() else [str(book_path)]

        if not audio_files:
            # No audio files - will mark as needs attention
            analysis_results.append((row, 'no_audio', {
                'log_message': f"[LAYER 3] No audio files found, marking needs attention: {path}"
            }))
            continue

        # Try audio analysis with Gemini (EXPENSIVE EXTERNAL CALL)
        # Use smart first-file detection for opening credits (title/author/narrator announcements)
        audio_result = analyze_audio_for_credits(str(book_path), config)

        if audio_result and audio_result.get('author') and audio_result.get('title'):
            # Audio analysis succeeded - but validate the response isn't garbage
            new_author = audio_result.get('author', '')
            new_title = audio_result.get('title', '')

            # VALIDATION: Reject obviously bad AI responses
            is_garbage = False
            garbage_reason = None

            # Check for garbage titles (too short, sentence fragments, etc.)
            if len(new_title) < 3:
                is_garbage = True
                garbage_reason = f"Title too short: '{new_title}'"
            elif new_title.lower().startswith(('the ', 'a ', 'an ')) and len(new_title) < 10:
                is_garbage = True
                garbage_reason = f"Title looks like fragment: '{new_title}'"
            elif len(new_title.split()) > 15:
                is_garbage = True
                garbage_reason = f"Title too long (looks like AI rambling): '{new_title[:50]}...'"
            elif any(phrase in new_title.lower() for phrase in ['presents', 'division of', 'recorded books', 'audio', 'penguin', 'random house', 'tantor']):
                is_garbage = True
                garbage_reason = f"Title contains publisher/format text: '{new_title[:50]}'"

            # Check for garbage authors
            if not is_garbage:
                if len(new_author) < 2:
                    is_garbage = True
                    garbage_reason = f"Author too short: '{new_author}'"
                elif len(new_author.split()) > 6:
                    is_garbage = True
                    garbage_reason = f"Author too long (looks like AI rambling): '{new_author[:50]}...'"
                elif any(phrase in new_author.lower() for phrase in ['written and read', 'presents', 'narrated by', 'audio', 'publisher']):
                    is_garbage = True
                    garbage_reason = f"Author contains non-name text: '{new_author[:50]}'"

            if is_garbage:
                logger.warning(f"[LAYER 3] Rejecting garbage AI response: {garbage_reason}")
                analysis_results.append((row, 'failed', {
                    'log_message': f"[LAYER 3] AI returned garbage, marking needs attention: {path}"
                }))
                continue

            new_narrator = audio_result.get('narrator', '')
            new_series = audio_result.get('series', '')
            new_series_num = audio_result.get('series_num')

            # Auto-save narrator to Skaldleita if we found one
            if new_narrator and auto_save_narrator:
                auto_save_narrator(new_narrator, source='audio_extract')

            # Contribute to community database (if opt-in enabled)
            if contribute_audio_extraction:
                contribute_audio_extraction(
                    title=new_title,
                    author=new_author,
                    narrator=new_narrator,
                    series=new_series,
                    series_position=new_series_num,
                    language=audio_result.get('language'),
                    confidence=audio_result.get('confidence', 'medium')
                )

            # Issue #57: Apply author initials standardization if enabled
            if config.get('standardize_author_initials', False) and new_author and standardize_initials:
                new_author = standardize_initials(new_author)

            logger.info(f"[LAYER 3] Audio extracted: {new_author}/{new_title} (narrator: {new_narrator})")

            current_author = row['current_author']
            current_title = row['current_title']

            # Check if extracted data differs from current values (returns 0.0-1.0)
            author_match = calculate_title_similarity(current_author, new_author) if current_author else 0
            title_match = calculate_title_similarity(current_title, new_title) if current_title else 0

            if author_match >= 0.90 and title_match >= 0.90:
                # Audio confirms current values are correct
                analysis_results.append((row, 'verified', {
                    'log_message': f"[LAYER 3] Audio confirms existing metadata, marking verified: {current_author}/{current_title}"
                }))
            else:
                # Audio suggests different values - build new path
                lib_path = None
                for lp in config.get('library_paths', []):
                    lp_path = Path(lp)
                    try:
                        book_path.relative_to(lp_path)
                        lib_path = lp_path
                        break
                    except ValueError:
                        continue

                # Issue #135: Route watch folder items to output folder
                watch_folder = config.get('watch_folder', '').strip()
                watch_output = config.get('watch_output_folder', '').strip()
                if watch_folder and watch_output and lib_path is None:
                    try:
                        if book_path.resolve().is_relative_to(Path(watch_folder).resolve()):
                            lib_path = Path(watch_output)
                            logger.info(f"[LAYER 3] Watch folder book: routing to output folder {lib_path}")
                    except Exception:
                        pass

                if lib_path is None:
                    lib_path = book_path.parent.parent
                    logger.warning(f"[LAYER 3] Book path {book_path} not under any configured library, guessing lib_path={lib_path}")

                # Detect language for multi-language naming
                lang_code = _detect_title_language(new_title)
                new_path = build_new_path(lib_path, new_author, new_title,
                                          series=new_series, series_num=new_series_num,
                                          narrator=new_narrator, language_code=lang_code, config=config)

                if new_path is None:
                    analysis_results.append((row, 'error', {
                        'log_message': f"[LAYER 3] SAFETY BLOCK: Invalid path for '{new_author}' / '{new_title}'",
                        'error_message': 'Audio extraction produced invalid path'
                    }))
                else:
                    analysis_results.append((row, 'pending_fix', {
                        'log_message': f"[LAYER 3] Creating pending fix: {current_author}/{current_title} -> {new_author}/{new_title}",
                        'new_author': new_author,
                        'new_title': new_title,
                        'new_narrator': new_narrator,
                        'new_series': new_series,
                        'new_series_num': new_series_num,
                        'new_path': str(new_path),
                        'book_path': str(book_path)
                    }))
        else:
            # Audio analysis failed
            analysis_results.append((row, 'failed', {
                'log_message': f"[LAYER 3] Audio analysis failed, marking needs attention: {path}"
            }))

    # === PHASE 3: Apply all updates (quick write, release connection) ===
    conn = get_db()
    c = conn.cursor()

    processed = 0
    resolved = 0

    for row, action_type, action_data in analysis_results:
        # Log the message
        if action_data.get('log_message'):
            if 'SAFETY BLOCK' in action_data['log_message'] or 'failed' in action_data['log_message'] or 'No audio' in action_data['log_message']:
                logger.warning(action_data['log_message'])
            else:
                logger.info(action_data['log_message'])

        if action_type == 'no_audio':
            c.execute('UPDATE books SET status = ?, verification_layer = 4, error_message = ? WHERE id = ?',
                     ('needs_attention', 'No audio files found for analysis', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))

        elif action_type == 'verified':
            c.execute('UPDATE books SET status = ?, verification_layer = 4 WHERE id = ?',
                     ('verified', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            resolved += 1

        elif action_type == 'error':
            c.execute('UPDATE books SET status = ?, verification_layer = 4, error_message = ? WHERE id = ?',
                     ('error', action_data['error_message'], row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))

        elif action_type == 'pending_fix':
            # Issue #79: Use helper function to prevent duplicates
            insert_history_entry(
                c, row['book_id'], row['current_author'], row['current_title'],
                action_data['new_author'], action_data['new_title'],
                action_data['book_path'], action_data['new_path'], 'pending_fix',
                error_message='Identified via audio analysis',
                new_narrator=action_data['new_narrator'], new_series=action_data['new_series'],
                new_series_num=str(action_data['new_series_num']) if action_data['new_series_num'] else None
            )
            c.execute('UPDATE books SET status = ?, verification_layer = 4 WHERE id = ?',
                     ('pending_fix', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            resolved += 1

        elif action_type == 'failed':
            # Layer 3 (credits) failed - advance to Layer 4 (content analysis) if enabled
            if config.get('enable_content_analysis', True):  # Enabled by default if audio analysis is on
                logger.info(f"Advancing to Layer 4 (credits analysis failed): {row['current_title']}")
                c.execute('UPDATE books SET verification_layer = 4 WHERE id = ?', (row['book_id'],))
                # Keep in queue for Layer 4
            else:
                c.execute('UPDATE books SET status = ?, verification_layer = 5, error_message = ? WHERE id = ?',
                         ('needs_attention', 'All verification layers exhausted - manual review required', row['book_id']))
                c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))

        processed += 1

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 3] Processed {processed}, resolved {resolved} via audio")
    return processed, resolved


__all__ = ['process_layer_3_audio']
