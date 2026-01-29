"""Layer 4: Content-Based Identification

The FINAL layer - analyzes actual story CONTENT to identify books.
Unlike Layer 3 (credits/intro), this:
- Extracts audio from the MIDDLE of the book (actual narration)
- Transcribes the spoken text
- Uses AI to identify the book from plot, characters, writing style

Best for:
- Multi-part files (Part 2, Part 3) that have no intro credits
- Books with music-only intros
- Files with cut/corrupted opening credits
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from library_manager.config import load_secrets
from library_manager.utils.validation import is_placeholder_author

logger = logging.getLogger(__name__)


def process_layer_4_content(
    config: Dict,
    get_db: Callable,
    analyze_audio_for_content: Callable,
    auto_save_narrator: Optional[Callable] = None,
    limit: Optional[int] = None
) -> Tuple[int, int]:
    """
    Layer 4: Content-Based Identification

    The FINAL layer - analyzes actual story CONTENT to identify books.

    Args:
        config: App configuration dict
        get_db: Function to get database connection
        analyze_audio_for_content: Function to analyze audio content
        auto_save_narrator: Optional function to save narrator info
        limit: Maximum batch size (overrides config)

    Returns:
        Tuple of (processed_count, resolved_count)
    """
    if not config.get('enable_audio_analysis', False):
        logger.info("[LAYER 4] Audio analysis disabled, skipping content analysis")
        return 0, 0

    # Check if we have Gemini API key
    secrets = load_secrets()
    if not secrets or not secrets.get('gemini_api_key'):
        logger.info("[LAYER 4] No Gemini API key for content analysis, skipping")
        return 0, 0

    # === PHASE 1: Fetch batch ===
    conn = get_db()
    c = conn.cursor()

    batch_size = limit or config.get('batch_size', 3)

    # Get items awaiting content analysis (layer 4)
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.verification_layer = 4
                   AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention', 'pending_fix')
                   AND (b.user_locked IS NULL OR b.user_locked = 0)
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (batch_size,))
    batch = [dict(row) for row in c.fetchall()]
    conn.close()

    if not batch:
        return 0, 0

    logger.info(f"[LAYER 4] Processing {len(batch)} items via content analysis")

    # === PHASE 2: Content analysis ===
    results = []  # (row, action_type, action_data)

    for row in batch:
        path = row['path']
        book_path = Path(path)

        # Analyze content from middle of the book
        content_result = analyze_audio_for_content(str(book_path), config)

        if content_result and content_result.get('author') and content_result.get('title'):
            # Don't accept low-confidence guesses for placeholder authors
            confidence = content_result.get('confidence', 'low')
            new_author = content_result.get('author', '')

            if is_placeholder_author(new_author):
                logger.warning(f"[LAYER 4] Content analysis returned placeholder '{new_author}', marking needs_attention")
                results.append((row, 'failed', {'log': f"Content analysis returned placeholder author: {new_author}"}))
                continue

            # Content analysis succeeded!
            new_title = content_result.get('title', '')
            new_series = content_result.get('series')
            new_series_num = content_result.get('series_num')
            new_narrator = content_result.get('narrator')

            # Auto-save narrator if found
            if new_narrator and auto_save_narrator:
                auto_save_narrator(new_narrator, source='content_extract')

            # Build new path
            lib_path = None
            for lp in config.get('library_paths', []):
                try:
                    book_path.relative_to(Path(lp))
                    lib_path = Path(lp)
                    break
                except ValueError:
                    continue

            if not lib_path:
                lib_path = book_path.parent.parent

            # Build target path with series grouping if applicable
            if new_series and config.get('series_grouping', True):
                series_num_str = f"{new_series_num} - " if new_series_num else ""
                new_path = lib_path / new_author / new_series / f"{series_num_str}{new_title}"
            else:
                new_path = lib_path / new_author / new_title

            # Add narrator suffix if enabled
            if new_narrator and config.get('include_narrator_in_filename', True):
                new_path = new_path.parent / f"{new_path.name} {{{new_narrator}}}"

            results.append((row, 'identified', {
                'new_author': new_author,
                'new_title': new_title,
                'new_series': new_series,
                'new_series_num': new_series_num,
                'new_narrator': new_narrator,
                'new_path': str(new_path),
                'book_path': str(book_path),
                'confidence': confidence,
                'reasoning': content_result.get('reasoning', '')
            }))
        else:
            # Content analysis failed - this is truly unidentifiable
            results.append((row, 'failed', {'log': 'Content analysis could not identify book'}))

    # === PHASE 3: Apply results ===
    conn = get_db()
    c = conn.cursor()

    processed = 0
    resolved = 0

    for row, action_type, action_data in results:
        if action_type == 'identified':
            logger.info(f"[LAYER 4] Creating pending fix: {row['current_author']}/{row['current_title']} -> "
                       f"{action_data['new_author']}/{action_data['new_title']} "
                       f"(confidence: {action_data['confidence']})")

            c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message,
                                              new_narrator, new_series, new_series_num)
                         VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix', ?, ?, ?, ?)''',
                     (row['book_id'], row['current_author'], row['current_title'],
                      action_data['new_author'], action_data['new_title'],
                      action_data['book_path'], action_data['new_path'],
                      f"Identified via content analysis: {action_data.get('reasoning', '')[:100]}",
                      action_data['new_narrator'], action_data['new_series'],
                      str(action_data['new_series_num']) if action_data['new_series_num'] else None))
            c.execute('UPDATE books SET status = ?, verification_layer = 5 WHERE id = ?',
                     ('pending_fix', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            resolved += 1

        elif action_type == 'failed':
            # All layers exhausted - truly needs manual review
            logger.warning(f"[LAYER 4] All layers exhausted, marking needs_attention: {row['current_title']}")
            c.execute('UPDATE books SET status = ?, verification_layer = 5, error_message = ? WHERE id = ?',
                     ('needs_attention', 'All verification layers (API, AI, credits, content) exhausted - manual review required', row['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))

        processed += 1

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 4] Processed {processed}, resolved {resolved} via content analysis")
    return processed, resolved


__all__ = ['process_layer_4_content']
