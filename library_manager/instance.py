"""Instance ID management for Library Manager.

Generates and persists a unique instance ID in the format SKALD-XXXXXX.
This ID survives config resets and updates, stored separately from config.json.
"""
import json
import secrets
import string
import logging
from pathlib import Path

from .config import DATA_DIR

logger = logging.getLogger(__name__)

# Instance file stored in data directory (survives container updates)
INSTANCE_PATH = DATA_DIR / 'instance.json'

# Instance ID format: "SKALD-" prefix (6 chars) + random suffix (6 chars) = 12 total
INSTANCE_ID_PREFIX = "SKALD-"
INSTANCE_ID_SUFFIX_LENGTH = 6
INSTANCE_ID_TOTAL_LENGTH = len(INSTANCE_ID_PREFIX) + INSTANCE_ID_SUFFIX_LENGTH  # 12


def _generate_instance_id() -> str:
    """Generate a new instance ID in format SKALD-XXXXXX (6 alphanumeric chars)."""
    chars = string.ascii_uppercase + string.digits
    suffix = ''.join(secrets.choice(chars) for _ in range(INSTANCE_ID_SUFFIX_LENGTH))
    return f"{INSTANCE_ID_PREFIX}{suffix}"


def get_instance_id() -> str:
    """Get or create the persistent instance ID.

    Returns the existing ID if present, otherwise generates a new one
    and persists it to instance.json.
    """
    if INSTANCE_PATH.exists():
        try:
            with open(INSTANCE_PATH, 'r') as f:
                data = json.load(f)
                instance_id = data.get('instance_id', '')
                if instance_id.startswith(INSTANCE_ID_PREFIX) and len(instance_id) == INSTANCE_ID_TOTAL_LENGTH:
                    return instance_id
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Error reading instance file: {e}")

    # Generate new ID
    instance_id = _generate_instance_id()
    save_instance_data({'instance_id': instance_id})
    logger.info(f"Generated new instance ID: {instance_id}")
    return instance_id


def save_instance_data(data: dict) -> None:
    """Save instance data to instance.json, merging with existing data."""
    existing = {}
    if INSTANCE_PATH.exists():
        try:
            with open(INSTANCE_PATH, 'r') as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    existing.update(data)
    try:
        with open(INSTANCE_PATH, 'w') as f:
            json.dump(existing, f, indent=2)
    except IOError as e:
        logger.error(f"Failed to save instance data: {e}")


def get_instance_data() -> dict:
    """Get all instance data including registration info."""
    if INSTANCE_PATH.exists():
        try:
            with open(INSTANCE_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {'instance_id': get_instance_id()}
