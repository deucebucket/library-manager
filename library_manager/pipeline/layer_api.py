"""Layer 1: API Database Lookups

Processes items using API databases (BookDB, Audnexus, OpenLibrary, etc.)
This is faster and cheaper than AI verification, so we try it first.
"""

import json
import logging
from collections import Counter
from typing import Callable, Dict, List, Optional, Tuple

from library_manager.models.book_profile import BookProfile
from library_manager.utils.naming import calculate_title_similarity, extract_series_from_title
from library_manager.utils.validation import is_placeholder_author

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

    Processes items at verification_layer=1 using API databases (BookDB, Audnexus, etc.)
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
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title, b.verification_layer
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

    for row in batch:
        current_author = row['current_author']
        current_title = row['current_title']

        # Update status bar with current book
        if set_current_book:
            set_current_book(current_author or 'Unknown', current_title or 'Unknown', "API lookup...")

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
                            profile.author.add_source(api_source, match_author)
                            profile.title.add_source(api_source, match_title)
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
                            # API found the book but current values differ - let Layer 2 handle the fix
                            action['type'] = 'advance_to_layer2'
                            action['log_message'] = f"[LAYER 1] API match needs fix ({avg_confidence:.0%}, title={title_sim:.0%}, author={author_sim:.0%}): {current_author}/{current_title} -> {match_author}/{match_title}"
                    else:
                        # Low confidence - advance to AI layer
                        action['type'] = 'advance_to_layer2'
                        action['log_message'] = f"[LAYER 1] API match low confidence ({avg_confidence:.0f}%), advancing to AI: {current_author}/{current_title}"
            else:
                # No good match found - advance to AI
                action['type'] = 'advance_to_layer2'
                action['log_message'] = f"[LAYER 1] No API match, advancing to AI: {current_author}/{current_title}"
        else:
            # No candidates at all - advance to AI
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

        if action['type'] == 'verified':
            # Save profile and mark as verified
            c.execute('UPDATE books SET status = ?, verification_layer = 4, profile = ?, confidence = ? WHERE id = ?',
                     ('verified', action['profile_json'], action['confidence'], action['book_id']))
            c.execute('DELETE FROM queue WHERE id = ?', (action['queue_id'],))
            resolved += 1
        elif action['type'] == 'advance_to_layer2':
            c.execute('UPDATE books SET verification_layer = 2 WHERE id = ?', (action['book_id'],))

        processed += 1

    conn.commit()
    conn.close()

    logger.info(f"[LAYER 1] Processed {processed}, resolved {resolved} via API")
    return processed, resolved


__all__ = ['process_layer_1_api']
