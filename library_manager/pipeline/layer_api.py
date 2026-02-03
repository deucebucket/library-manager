"""Layer 1: API Database Lookups

Processes items using API databases (Skaldleita, Audnexus, OpenLibrary, etc.)
This is faster and cheaper than AI verification, so we try it first.
"""

import json
import logging
from collections import Counter
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from library_manager.models.book_profile import BookProfile
from library_manager.utils.naming import calculate_title_similarity, extract_series_from_title
from library_manager.utils.validation import is_placeholder_author
from library_manager.worker import set_current_provider

logger = logging.getLogger(__name__)


def process_layer_1_api(
    config: Dict,
    get_db: Callable,
    gather_all_api_candidates: Callable,
    limit: Optional[int] = None,
    set_current_book: Optional[Callable] = None
) -> Tuple[int, int]:
    """
    Layer 1: API Database Lookups

    Processes items at verification_layer=1 using API databases (Skaldleita, Audnexus, etc.)
    Items that get a confident match are marked complete.
    Items that fail are advanced to layer 2 (AI verification).

    Args:
        config: App configuration dict
        get_db: Function to get database connection
        gather_all_api_candidates: Function to gather candidates from all APIs
        limit: Maximum batch size (overrides config)

    Returns:
        Tuple of (processed_count, resolved_count)

    NOTE: This function uses a 3-phase approach to avoid holding DB locks during
    external API calls (which can take 10-30+ seconds for a batch):
      Phase 1: Quick fetch, release connection
      Phase 2: External API work (no DB connection held)
      Phase 3: Quick write, release connection
    """
    if not config.get('enable_api_lookups', True):
        logger.info("[LAYER 1] API lookups disabled, skipping")
        return 0, 0

    # === PHASE 1: Fetch batch (quick read, release connection immediately) ===
    conn = get_db()
    c = conn.cursor()

    batch_size = limit or config.get('batch_size', 3)
    confidence_threshold = config.get('profile_confidence_threshold', 85)

    # Get items awaiting API lookup (layer 1) or new items (layer 0)
    # Skip user-locked books - user has manually set metadata
    # Include profile and confidence for SL trust mode checks
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title, b.verification_layer,
                        b.profile, b.confidence
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.verification_layer IN (0, 1)
                   AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                   AND (b.user_locked IS NULL OR b.user_locked = 0)
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (batch_size,))
    # Convert to dicts immediately - sqlite3.Row objects become invalid after conn.close()
    batch = [dict(row) for row in c.fetchall()]
    conn.close()  # Release DB lock BEFORE external API calls

    if not batch:
        return 0, 0

    logger.info(f"[LAYER 1] Processing {len(batch)} items via API lookup")

    # === PHASE 2: External API lookups (NO database connection held) ===
    # Collect all actions to perform, then apply them in phase 3
    actions = []  # List of action dicts describing what to do for each item

    # Get SL trust mode settings
    sl_trust_mode = config.get('sl_trust_mode', 'full')
    sl_threshold = config.get('sl_confidence_threshold', 80)

    # System folder patterns that should never be processed as books
    garbage_inputs = {
        '@eadir', '#recycle', '@syno', '@tmp',
        '.appledouble', '__macosx', '.ds_store', '.spotlight', '.fseventsd', '.trashes',
        '$recycle.bin', 'system volume information', 'thumbs.db',
        '.trash', '.cache', '.metadata', '.thumbnails',
        'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
        'backup', 'backups', '.streams', 'streams'
    }

    for row in batch:
        current_author = row['current_author']
        current_title = row['current_title']

        # === GARBAGE INPUT CHECK ===
        # Reject system folders/garbage before wasting API calls or AI time
        title_lower = (current_title or '').lower().strip()
        author_lower = (current_author or '').lower().strip()

        is_garbage_input = False
        if title_lower in garbage_inputs or author_lower in garbage_inputs:
            is_garbage_input = True
        elif title_lower.startswith('@') or title_lower.startswith('#'):
            is_garbage_input = True
        elif author_lower.startswith('@') or author_lower.startswith('#'):
            is_garbage_input = True

        if is_garbage_input:
            # Mark as needs_attention so user can delete, don't send to AI
            action = {
                'book_id': row['book_id'],
                'queue_id': row['queue_id'],
                'type': 'garbage_rejected',
                'profile_json': None,
                'confidence': 0,
                'log_message': f"[LAYER 1] REJECTED garbage input (system folder): {current_author}/{current_title}"
            }
            actions.append(action)
            continue

        # === SL TRUST MODE CHECK ===
        # If book already identified by SL audio with high confidence, trust it
        book_profile = row.get('profile')
        book_confidence = row.get('confidence', 0) or 0

        if sl_trust_mode in ('full', 'boost') and book_profile and book_confidence >= sl_threshold:
            try:
                profile_data = json.loads(book_profile) if isinstance(book_profile, str) else book_profile
                author_data = profile_data.get('author', {})
                author_source = author_data.get('source', '')

                # Only trust audio-based sources (not folder-derived)
                audio_sources = ('audio_transcription', 'bookdb', 'bookdb_audio', 'audio')
                if author_source in audio_sources:
                    # Book was identified by SL audio - trust it completely
                    action = {
                        'book_id': row['book_id'],
                        'queue_id': row['queue_id'],
                        'type': 'trust_sl',
                        'profile_json': None,
                        'confidence': book_confidence,
                        'log_message': f"[LAYER 1] Trusting SL audio ID (conf={book_confidence}%): {current_author}/{current_title}"
                    }
                    actions.append(action)
                    continue  # Skip API lookups entirely - will be counted in Phase 3
            except (json.JSONDecodeError, TypeError):
                pass  # Invalid profile, proceed with normal flow

        # Update status bar with current book
        if set_current_book:
            set_current_book(current_author or 'Unknown', current_title or 'Unknown', "API lookup...")

        # Show we're using multiple free APIs for lookup
        set_current_provider("BookDB + APIs", "Querying Audnexus, OpenLibrary, Google Books...", is_free=True)

        # Use existing API candidate gathering function (EXTERNAL CALLS HAPPEN HERE)
        candidates = gather_all_api_candidates(current_title, current_author, config)

        # Determine what action to take based on API results
        action = {
            'book_id': row['book_id'],
            'queue_id': row['queue_id'],
            'type': None,  # Will be set below
            'profile_json': None,
            'confidence': None,
            'log_message': None
        }

        if candidates:
            # Issue #57 (Merijeek): Vote by author popularity across APIs
            # If 6 APIs say "Charles Stross" and 1 says "China Mieville", Charles Stross should win

            # Normalize author names for comparison (lowercase, strip whitespace)
            def normalize_author(a):
                return (a or '').lower().strip()

            # Count votes for each author
            author_votes = Counter()
            author_to_candidate = {}  # Map normalized author -> best candidate with that author

            for candidate in candidates:
                norm_author = normalize_author(candidate.get('author'))
                if norm_author and norm_author not in ['unknown', 'various', 'various authors', 'n/a', 'none']:
                    author_votes[norm_author] += 1
                    # Keep track of the candidate (prefer ones with series info)
                    existing = author_to_candidate.get(norm_author)
                    if not existing or (candidate.get('series') and not existing.get('series')):
                        author_to_candidate[norm_author] = candidate

            best_match = None

            if author_votes:
                # Get the most common author
                most_common_author, vote_count = author_votes.most_common(1)[0]

                # If current author matches the winner OR has more than 1 vote, use the winner
                current_norm = normalize_author(current_author)

                if vote_count > 1 or is_placeholder_author(current_author):
                    # Multiple APIs agree OR current author is placeholder - trust the vote
                    best_match = author_to_candidate.get(most_common_author)
                    if vote_count > 1:
                        logger.debug(f"[LAYER 1] Author vote: '{most_common_author}' won with {vote_count} votes")
                elif current_norm == most_common_author:
                    # Current author matches the winner - good!
                    best_match = author_to_candidate.get(most_common_author)
                elif current_norm in author_to_candidate:
                    # Current author is in candidates but didn't win - still use it if only 1 vote each
                    # This handles ties gracefully
                    best_match = author_to_candidate.get(current_norm)
                else:
                    # Current author not in candidates at all - use the vote winner
                    best_match = author_to_candidate.get(most_common_author)

            if not best_match:
                best_match = candidates[0]  # Ultimate fallback

            # Check if this is a good enough match
            match_title = best_match.get('title', '')
            match_author = best_match.get('author', '')

            if match_title and match_author:
                # Calculate match confidence using word overlap similarity (returns 0.0-1.0)
                title_sim = calculate_title_similarity(current_title, match_title) if current_title else 0

                # IMPORTANT: If author is placeholder (Unknown, Various, etc.), we CANNOT verify as-is
                # The book needs to be fixed, not verified. Advance to Layer 2 for proper identification.
                if is_placeholder_author(current_author):
                    action['type'] = 'advance_to_layer2'
                    action['log_message'] = f"[LAYER 1] Placeholder author '{current_author}', advancing to AI for identification: {current_title}"
                else:
                    author_sim = calculate_title_similarity(current_author, match_author)
                    avg_confidence = (title_sim + author_sim) / 2

                    # confidence_threshold is 0-100 scale, convert to 0-1
                    threshold = confidence_threshold / 100.0 if confidence_threshold > 1 else confidence_threshold
                    if avg_confidence >= threshold:
                        # Good match found - check if current values are correct or need fixing
                        if title_sim >= 0.90 and author_sim >= 0.90:
                            # Book is already correctly named - mark as verified and remove from queue
                            action['type'] = 'verified'
                            action['log_message'] = f"[LAYER 1] Verified OK ({avg_confidence:.0%}): {current_author}/{current_title}"

                            # Create profile with verification source
                            api_source = best_match.get('source', 'api')
                            profile = BookProfile()
                            profile.add_author(api_source, match_author)
                            profile.add_title(api_source, match_title)
                            if best_match.get('series'):
                                profile.series.add_source(api_source, best_match['series'])
                            if best_match.get('series_num'):
                                profile.series_num.add_source(api_source, best_match['series_num'])

                            # Issue #57: Fallback - extract series from title if API didn't provide it
                            # This ensures consistent series detection between Layer 1 and Layer 2
                            if not best_match.get('series'):
                                extracted_series, extracted_num, _ = extract_series_from_title(match_title)
                                if extracted_series:
                                    profile.series.add_source('path', extracted_series)
                                    if extracted_num:
                                        profile.series_num.add_source('path', extracted_num)

                            profile.verification_layers_used = ['api']
                            profile.finalize()

                            action['profile_json'] = json.dumps(profile.to_dict())
                            action['confidence'] = profile.overall_confidence
                        else:
                            # API found the book but current values differ
                            # Respect SL trust mode: in full/boost mode, skip AI
                            if sl_trust_mode == 'full':
                                action['type'] = 'advance_to_layer4'
                                action['log_message'] = f"[LAYER 1] API match needs fix, trust mode=full, skipping AI: {current_author}/{current_title} -> {match_author}/{match_title}"
                            elif sl_trust_mode == 'boost':
                                # In boost mode, use API confidence to decide
                                if avg_confidence >= 0.7:  # 70%+ = trust API, skip AI
                                    action['type'] = 'advance_to_layer4'
                                    action['log_message'] = f"[LAYER 1] API match ({avg_confidence:.0%}), boost mode, skipping AI: {current_author}/{current_title}"
                                else:
                                    action['type'] = 'advance_to_layer2'
                                    action['log_message'] = f"[LAYER 1] API match low ({avg_confidence:.0%}), boost mode, using AI: {current_author}/{current_title}"
                            else:  # legacy mode
                                action['type'] = 'advance_to_layer2'
                                action['log_message'] = f"[LAYER 1] API match needs fix ({avg_confidence:.0%}), legacy mode: {current_author}/{current_title} -> {match_author}/{match_title}"
                    else:
                        # Low confidence - respect trust mode
                        if sl_trust_mode in ('full', 'boost'):
                            action['type'] = 'advance_to_layer4'
                            action['log_message'] = f"[LAYER 1] API match low confidence ({avg_confidence:.0f}%), trust mode={sl_trust_mode}, skipping AI: {current_author}/{current_title}"
                        else:
                            action['type'] = 'advance_to_layer2'
                            action['log_message'] = f"[LAYER 1] API match low confidence ({avg_confidence:.0f}%), advancing to AI: {current_author}/{current_title}"
            else:
                # No good match found - respect trust mode
                if sl_trust_mode in ('full', 'boost'):
                    action['type'] = 'advance_to_layer4'
                    action['log_message'] = f"[LAYER 1] No API match, trust mode={sl_trust_mode}, skipping AI: {current_author}/{current_title}"
                else:
                    action['type'] = 'advance_to_layer2'
                    action['log_message'] = f"[LAYER 1] No API match, advancing to AI: {current_author}/{current_title}"
        else:
            # No candidates at all - respect trust mode
            if sl_trust_mode in ('full', 'boost'):
                action['type'] = 'advance_to_layer4'
                action['log_message'] = f"[LAYER 1] No API candidates, trust mode={sl_trust_mode}, skipping AI: {current_author}/{current_title}"
            else:
                action['type'] = 'advance_to_layer2'
                action['log_message'] = f"[LAYER 1] No API candidates, advancing to AI: {current_author}/{current_title}"

        actions.append(action)

    # === PHASE 3: Apply all updates (quick write, release connection) ===
    conn = get_db()
    c = conn.cursor()

    processed = 0
    resolved = 0

    for action in actions:
        # Log the message (was logged inline before, now batched)
        if action['log_message']:
            logger.info(action['log_message'])

        if action['type'] == 'trust_sl':
            # SL audio ID was high confidence - mark as resolved, skip Layer 2 (AI)
            # Advance to Layer 4 for final verification/fix application
            c.execute('UPDATE books SET verification_layer = 4 WHERE id = ?', (action['book_id'],))
            c.execute('DELETE FROM queue WHERE id = ?', (action['queue_id'],))
            resolved += 1
        elif action['type'] == 'verified':
            # Save profile and mark as verified
            c.execute('UPDATE books SET status = ?, verification_layer = 4, profile = ?, confidence = ? WHERE id = ?',
                     ('verified', action['profile_json'], action['confidence'], action['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (action['queue_id'],))
            resolved += 1
        elif action['type'] == 'advance_to_layer4':
            # Skip Layer 2 (AI), go directly to Layer 4 (final verification/fix)
            c.execute('UPDATE books SET verification_layer = 4 WHERE id = ?', (action['book_id'],))
        elif action['type'] == 'advance_to_layer2':
            c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (action['book_id'],))
        elif action['type'] == 'garbage_rejected':
            # System folder/garbage input - mark for user cleanup, don't process further
            c.execute('''UPDATE books SET status = 'needs_attention',
                        error_message = 'System folder detected - remove from library',
                        verification_layer = 4 WHERE id = ?''', (action['book_id'],))
            c.execute('DELETE FROM queue WHERE id = ?', (action['queue_id'],))

        processed += 1

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 1] Processed {processed}, resolved {resolved} via API")
    return processed, resolved


def process_sl_requeue_verification(
    config: Dict,
    get_db: Callable,
    search_bookdb: Callable,
    limit: Optional[int] = None
) -> Tuple[int, int]:
    """
    Phase 5: Verify books that were scheduled for SL requeue.

    When SL returns requeue_suggested=true (live scrape in progress),
    we store the partial ID and schedule re-verification for after nightly merge.

    This function finds those books and re-verifies them against SL to see
    if the book is now in the main database with higher confidence.

    Args:
        config: App configuration dict
        get_db: Function to get database connection
        search_bookdb: Function to search Skaldleita
        limit: Maximum batch size

    Returns:
        Tuple of (processed_count, upgraded_count)
    """
    conn = get_db()
    c = conn.cursor()

    batch_size = limit or config.get('batch_size', 5)

    # Find books with sl_requeue where requeue_after has passed
    # These are pending_fix books that need re-verification
    c.execute('''SELECT id, path, current_author, current_title, profile, confidence
                 FROM books
                 WHERE status = 'pending_fix'
                   AND profile IS NOT NULL
                   AND profile LIKE '%sl_requeue%'
                 LIMIT ?''', (batch_size,))

    books_to_check = [dict(row) for row in c.fetchall()]
    conn.close()

    if not books_to_check:
        return 0, 0

    processed = 0
    upgraded = 0

    for book in books_to_check:
        try:
            profile = json.loads(book['profile']) if book['profile'] else {}
            sl_requeue = profile.get('sl_requeue', {})

            if not sl_requeue:
                continue

            # Check if requeue time has passed
            requeue_after_str = sl_requeue.get('requeue_after')
            if not requeue_after_str:
                continue

            requeue_after = datetime.fromisoformat(requeue_after_str)
            if datetime.now() < requeue_after:
                continue  # Not time yet

            # Time to re-verify! Query Skaldleita
            author = book['current_author']
            title = book['current_title']

            logger.info(f"[SL REQUEUE] Re-verifying: {author} - {title}")

            # Search Skaldleita for this book
            sl_results = search_bookdb(
                title=title,
                author=author,
                include_editions=False,
                limit=5
            )

            if sl_results and len(sl_results) > 0:
                # Found in main DB now - upgrade confidence
                best_match = sl_results[0]
                new_confidence = min(95, book['confidence'] + 10)

                # Update profile - remove requeue flag, add SL verification
                profile.pop('sl_requeue', None)
                profile['sl_verified'] = {
                    'book_id': best_match.get('id'),
                    'verified_at': datetime.now().isoformat(),
                    'confidence_boost': 10
                }

                conn = get_db()
                c = conn.cursor()
                c.execute('''UPDATE books SET profile = ?, confidence = ? WHERE id = ?''',
                         (json.dumps(profile), new_confidence, book['id']))
                conn.commit()
                conn.close()

                logger.info(f"[SL REQUEUE] Upgraded confidence {book['confidence']} -> {new_confidence}: {author} - {title}")
                upgraded += 1
            else:
                # Still not in main DB - remove requeue flag, keep current identification
                profile.pop('sl_requeue', None)
                profile['sl_requeue_complete'] = {
                    'checked_at': datetime.now().isoformat(),
                    'result': 'not_found_in_main_db'
                }

                conn = get_db()
                c = conn.cursor()
                c.execute('UPDATE books SET profile = ? WHERE id = ?',
                         (json.dumps(profile), book['id']))
                conn.commit()
                conn.close()

                logger.info(f"[SL REQUEUE] No upgrade, keeping current ID: {author} - {title}")

            processed += 1

        except Exception as e:
            logger.warning(f"[SL REQUEUE] Error processing book {book['id']}: {e}")
            processed += 1

    logger.info(f"[SL REQUEUE] Processed {processed}, upgraded {upgraded}")
    return processed, upgraded


__all__ = ['process_layer_1_api', 'process_sl_requeue_verification']
