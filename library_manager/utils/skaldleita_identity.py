"""Explicit Skaldleita book identity, separate from ASIN/ISBN identifiers."""

import json
import re
from urllib.parse import urlsplit, urlunsplit


def canonical_skaldleita_book_id(value):
    """Return a positive decimal string without guessing other ID namespaces."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    text = str(value)
    if not re.fullmatch(r"[0-9]{1,20}", text):
        return None
    number = int(text)
    return str(number) if number > 0 else None


def canonical_skaldleita_source_url(value):
    """Normalize the configured endpoint; never persist URL credentials."""
    if not isinstance(value, str) or any(ord(char) < 33 or ord(char) == 127 for char in value):
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment):
            return None
        host = parsed.hostname.lower()
        if ":" in host:
            host = "[" + host + "]"
        port = parsed.port
        if port is not None and (parsed.scheme, port) not in (("http", 80), ("https", 443)):
            host += ":" + str(port)
        return urlunsplit((parsed.scheme, host, parsed.path.rstrip("/"), "", ""))
    except ValueError:
        return None


def skaldleita_identity(payload):
    """Read only an explicit ID and its locally recorded endpoint provenance."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (ValueError, TypeError):
            return {}
    if not isinstance(payload, dict):
        return {}
    book_id = canonical_skaldleita_book_id(payload.get("skaldleita_book_id"))
    source = canonical_skaldleita_source_url(payload.get("skaldleita_source_url"))
    if book_id is None or source is None:
        return {}
    return {"skaldleita_book_id": book_id, "skaldleita_source_url": source}


def provider_skaldleita_identity(payload, endpoint):
    """Bind the server's dedicated field to the actual configured endpoint."""
    if not isinstance(payload, dict):
        return {}
    return skaldleita_identity({
        "skaldleita_book_id": payload.get("skaldleita_book_id"),
        "skaldleita_source_url": endpoint,
    })
