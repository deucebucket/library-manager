"""
Skaldleita request signing - shared constants and derivation.

This file is the source of truth for request signing between Library Manager
and Skaldleita. Skaldleita fetches this file to stay in sync.

Fetch URL: https://raw.githubusercontent.com/deucebucket/library-manager/develop/library_manager/signing.py
"""

import hashlib
import hmac
import time
import uuid

# Signing salt - combined with version to derive per-release secret
# Change this to invalidate ALL existing signatures (nuclear option)
SIGNING_SALT = 'skaldleita-lm-2024'

# How many recent versions Skaldleita should accept
ACCEPTED_VERSION_COUNT = 5

# Timestamp tolerance in seconds (reject requests with old timestamps)
TIMESTAMP_TOLERANCE = 120  # 2 minutes


def derive_secret(version: str) -> str:
    """Derive signing secret from version. Must match in both LM and Skaldleita."""
    return hashlib.sha256(f"{SIGNING_SALT}:{version}".encode()).hexdigest()[:32]


def generate_nonce() -> str:
    """Generate a random nonce (UUID4 hex) to prevent request replay."""
    return uuid.uuid4().hex


def generate_signature(version: str, timestamp: str, nonce: str = None) -> str:
    """Generate HMAC signature for a request. Nonce is optional for backwards compatibility."""
    secret = derive_secret(version)
    message = f"{timestamp}:{version}"
    if nonce:
        message = f"{timestamp}:{version}:{nonce}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()[:32]


def verify_signature(signature: str, version: str, timestamp: str, nonce: str = None) -> bool:
    """Verify a signature (for Skaldleita server-side use). Nonce is optional for backwards compatibility."""
    expected = generate_signature(version, timestamp, nonce=nonce)
    return hmac.compare_digest(signature, expected)


# For Skaldleita to import/fetch
__all__ = [
    'SIGNING_SALT',
    'ACCEPTED_VERSION_COUNT',
    'TIMESTAMP_TOLERANCE',
    'derive_secret',
    'generate_nonce',
    'generate_signature',
    'verify_signature',
]
