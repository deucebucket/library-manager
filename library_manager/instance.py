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


def _generate_instance_id() -> str:
    """Generate a new instance ID in format SKALD-XXXXXX (6 alphanumeric chars)."""
    chars = string.ascii_uppercase + string.digits
    suffix = ''.join(secrets.choice(chars) for _ in range(6))
    return f"SKALD-{suffix}"


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
                if instance_id.startswith('SKALD-') and len(instance_id) == 12:
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
