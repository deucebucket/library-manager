"""Bounded corrections transport using the existing Skaldleita auth contract."""

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import time
from types import SimpleNamespace
from typing import Optional

import requests

from library_manager.providers import bookdb
from library_manager.utils.skaldleita_identity import canonical_skaldleita_source_url


MAX_PAGE_BYTES = 2 * 1024 * 1024
MAX_CURSOR_LENGTH = 8192
MAX_RETRY_AFTER = 30 * 24 * 60 * 60


class FeedFailure(Exception):
    """A bounded public outcome, never an upstream response or credential."""

    def __init__(self, code, *, status=None, retry_after=None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.retry_after = retry_after


@dataclass(frozen=True)
class CorrectionPage:
    corrections: tuple
    next_cursor: str
    has_more: bool


def source_namespace(endpoint):
    source = canonical_skaldleita_source_url(endpoint)
    if source is None:
        raise FeedFailure('source_invalid')
    return source


def validate_since(value):
    """Require an explicit timezone when a bootstrap timestamp is supplied."""
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 64 or 'T' not in value:
        raise FeedFailure('since_invalid')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise FeedFailure('since_invalid') from None
    if parsed.tzinfo is None:
        raise FeedFailure('since_invalid')
    return value


def validate_cursor(value):
    if (not isinstance(value, str) or not value or len(value) > MAX_CURSOR_LENGTH
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise FeedFailure('cursor_invalid')
    return value


def _retry_after(value, now):
    if not isinstance(value, str) or len(value) > 100:
        return None
    try:
        seconds = int(value)
    except ValueError:
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                return None
            seconds = int((deadline - datetime.fromtimestamp(now, timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return None
    # Implausible guidance is invalid, not an effectively permanent suspension.
    # The scheduler supplies its ordinary bounded backoff when this is absent.
    return seconds if 0 <= seconds <= MAX_RETRY_AFTER else None


def _read_body(response):
    chunks = []
    size = 0
    for chunk in response.iter_content(chunk_size=16384):
        size += len(chunk)
        if size > MAX_PAGE_BYTES:
            raise FeedFailure('response_too_large', status=response.status_code)
        chunks.append(chunk)
    return b''.join(chunks)


def fetch_page(endpoint, *, api_key=None, cursor=None, since=None, limit=100,
               session=None, now=None):
    """Fetch one page only. Caller owns opt-in, scheduling and checkpoints.

    Authentication uses the same single preselected credential and terminal
    denial state as existing metadata/audio workflows. Redirects are refused.
    """
    source = source_namespace(endpoint)
    now = time.time() if now is None else now
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise FeedFailure('limit_invalid')
    if cursor is not None and since is not None:
        raise FeedFailure('cursor_since_conflict')
    params = {'limit': limit}
    if cursor is not None:
        params['cursor'] = validate_cursor(cursor)
    elif since is not None:
        params['since'] = validate_since(since)
    if bookdb.get_terminal_server_denial():
        raise FeedFailure('authentication_terminal')
    client = session if session is not None else requests
    response = None
    try:
        response = client.get(
            source + '/api/corrections', params=params,
            headers=bookdb.get_bookdb_headers(api_key), timeout=(5, 20),
            stream=True, allow_redirects=False,
        )
        status = response.status_code
        if status in (401, 403):
            # Do not let the shared denial parser consume an unbounded stream.
            # The terminal policy applies even if the denial body is oversized.
            try:
                body = _read_body(response)
            except (FeedFailure, requests.RequestException):
                body = b''
            bounded = SimpleNamespace(status_code=status, content=body,
                                      json=lambda: json.loads(body))
            bookdb.handle_terminal_auth_response(bounded, 'SKALDLEITA CORRECTIONS')
            raise FeedFailure('authentication_terminal', status=status)
        if status != 200:
            code = {404: 'feature_unavailable', 422: 'query_invalid',
                    429: 'rate_limited', 503: 'store_unavailable'}.get(status, 'http_error')
            if status == 400:
                try:
                    error = json.loads(_read_body(response))
                    detail = error.get('detail', {}) if isinstance(error, dict) else {}
                except (ValueError, UnicodeDecodeError):
                    detail = {}
                if isinstance(detail, dict):
                    if (detail.get('code') == 'corrections_cursor_reset_required'
                            and detail.get('reset_required') is True and cursor is not None):
                        code = 'bootstrap_required'
                    elif detail.get('code') == 'invalid_corrections_cursor':
                        code = 'cursor_invalid'
                    elif detail.get('code') == 'invalid_corrections_query':
                        code = 'query_invalid'
            raise FeedFailure(code, status=status,
                              retry_after=_retry_after(response.headers.get('Retry-After'), now))
        body = _read_body(response)
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            raise FeedFailure('page_invalid') from None
        if not isinstance(payload, dict):
            raise FeedFailure('page_invalid')
        corrections = payload.get('corrections')
        if (not isinstance(corrections, list) or len(corrections) > limit
                or any(not isinstance(item, dict) for item in corrections)
                or type(payload.get('has_more')) is not bool):
            raise FeedFailure('page_invalid')
        checkpoint = validate_cursor(payload.get('next_cursor'))
        return CorrectionPage(tuple(corrections), checkpoint, payload['has_more'])
    except requests.RequestException:
        raise FeedFailure('transport_unavailable') from None
    finally:
        if response is not None:
            response.close()


@dataclass(frozen=True)
class ValidatedCorrection:
    """Output of a caller-supplied canonical backend event-schema validator.

    A stable event ID is normalized by that validator, not guessed by transport.
    Payload must contain only validated catalog fields, never arbitrary raw data.
    """

    event_id: str
    payload: dict


@dataclass(frozen=True)
class FeedCheckpoint:
    source: str
    version: int = 0
    cursor: Optional[str] = None
    since: Optional[str] = None
    has_more: bool = False
    bootstrap_required: bool = False
    next_poll_at: float = 0
    last_error: Optional[str] = None
    generation: int = 0
    lease_owner: Optional[str] = None
    lease_expires: float = 0
