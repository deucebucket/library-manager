"""Corrections v1 validation pinned to Skaldleita e4feef29 (PR #185).

Source: docs/CORRECTIONS_API.md and src/schemas/corrections.py. Preserve patch
omission versus explicit null; no legacy provider field aliases are accepted.
"""

import math
import re

from .feed import CorrectionPage, FeedFailure, ValidatedCorrection, validate_since


METADATA_FIELDS = frozenset({
    'skaldleita_book_id', 'sl_id', 'title', 'author', 'narrator', 'series',
    'series_position', 'asin', 'language', 'year',
})
REASONS = frozenset({'narrator_mismatch', 'wrong_book', 'merged_duplicate', 'data_quality', 'manual'})
TARGET_FIELDS = ('affected_skaldleita_book_ids', 'affected_sl_ids', 'affected_audio_hashes')
EVENT_FIELDS = frozenset({'id', 'created_at', 'reason', 'original_result', 'corrected_result', *TARGET_FIELDS})
IDENTIFIER = re.compile(r'[A-Za-z0-9_.:-]{1,128}\Z')
AUDIO_HASH = re.compile(r'[0-9a-f]{64}\Z')


def _positive_integer(value):
    return type(value) is int and value > 0


def validate_metadata(value):
    if not isinstance(value, dict) or not set(value).issubset(METADATA_FIELDS):
        raise FeedFailure('event_invalid')
    for field, item in value.items():
        if item is None:
            continue
        if field == 'skaldleita_book_id':
            valid = _positive_integer(item)
        elif field == 'year':
            valid = type(item) is int
        elif field == 'series_position':
            try:
                valid = type(item) in (int, float) and math.isfinite(item)
            except OverflowError:
                valid = False
        elif field == 'sl_id':
            valid = isinstance(item, str) and IDENTIFIER.fullmatch(item) is not None
        else:
            valid = isinstance(item, str) and len(item) <= (32 if field in ('asin', 'language') else 1000)
        if not valid:
            raise FeedFailure('event_invalid')
    return dict(value)


def validate_event(raw):
    if not isinstance(raw, dict) or set(raw) != EVENT_FIELDS or not _positive_integer(raw['id']):
        raise FeedFailure('event_invalid')
    if not isinstance(raw['reason'], str) or raw['reason'] not in REASONS:
        raise FeedFailure('event_invalid')
    try:
        validate_since(raw['created_at'])
    except FeedFailure:
        raise FeedFailure('event_invalid') from None
    if raw['created_at'] is None:
        raise FeedFailure('event_invalid')
    payload = dict(raw)
    for name in TARGET_FIELDS:
        targets = raw[name]
        if not isinstance(targets, list) or len(targets) > 500:
            raise FeedFailure('event_invalid')
        for item in targets:
            valid = _positive_integer(item) if name == 'affected_skaldleita_book_ids' else (
                isinstance(item, str) and
                (IDENTIFIER if name == 'affected_sl_ids' else AUDIO_HASH).fullmatch(item) is not None)
            if not valid:
                raise FeedFailure('event_invalid')
        payload[name] = list(targets)
    if not any(payload[name] for name in TARGET_FIELDS):
        raise FeedFailure('event_invalid')
    before = validate_metadata(raw['original_result'])
    after = validate_metadata(raw['corrected_result'])
    if not after or before == after:
        raise FeedFailure('event_invalid')
    payload['original_result'] = before
    payload['corrected_result'] = after
    return ValidatedCorrection(str(raw['id']), payload)


def validate_page(page):
    """Reject a whole page before ingestion if event IDs are not increasing."""
    if not isinstance(page, CorrectionPage):
        raise FeedFailure('page_invalid')
    previous = 0
    for raw in page.corrections:
        event = validate_event(raw)
        event_id = int(event.event_id)
        if event_id <= previous:
            raise FeedFailure('page_invalid')
        previous = event_id
    return page
