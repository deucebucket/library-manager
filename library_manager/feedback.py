"""User feedback and session logging for Library Manager.

Provides:
- Session action logger (circular buffer of recent user actions)
- Path/data sanitizer (strips sensitive info)
- Feedback storage (local JSON, structured for future Skaldleita API forwarding)
"""
import json
import re
import platform
import sys
import logging
import traceback as tb_module
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import Lock

from .config import DATA_DIR

logger = logging.getLogger(__name__)

FEEDBACK_PATH = DATA_DIR / "feedback.json"
MAX_FEEDBACK_ENTRIES = 200
MAX_SESSION_LOG_SIZE = 50

# Thread-safe session log
_session_log = deque(maxlen=MAX_SESSION_LOG_SIZE)
_session_lock = Lock()

# Patterns to sanitize
_PATH_PATTERN = re.compile(
    r'(/home/[^/\s"\']+|/Users/[^/\s"\']+|/mnt/[^/\s"\']+|'
    r'/data|/audiobooks|[A-Z]:\\[^\s"\']+)'
)
_IP_PATTERN = re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b')
_API_KEY_PATTERN = re.compile(
    r'(sk-[a-zA-Z0-9]{20,}|AIza[a-zA-Z0-9_-]{20,}|'
    r'xkeysib-[a-zA-Z0-9-]+|Bearer\s+[a-zA-Z0-9._-]{20,})'
)


def sanitize_string(s):
    """Remove sensitive data from a string (paths, IPs, API keys)."""
    if not s:
        return s
    s = _PATH_PATTERN.sub('[PATH]', str(s))
    s = _IP_PATTERN.sub('[IP]', s)
    s = _API_KEY_PATTERN.sub('[REDACTED]', s)
    return s


def log_action(action_type, detail=None, book=None, path=None, result=None):
    """Log a user action to the session buffer.

    Args:
        action_type: Short action name (e.g., "scan", "apply_fix", "process")
        detail: Optional extra detail string
        book: Optional book title/author string
        path: Optional affected path (will be sanitized)
        result: Optional result string ("success", "error", etc.)
    """
    entry = {
        "ts": datetime.now().isoformat(),
        "action": action_type,
    }
    if detail:
        entry["detail"] = str(detail)[:200]
    if book:
        entry["book"] = str(book)[:150]
    if path:
        entry["path"] = sanitize_string(str(path))
    if result:
        entry["result"] = str(result)[:100]

    with _session_lock:
        _session_log.append(entry)


def get_session_log():
    """Return a copy of the current session log."""
    with _session_lock:
        return list(_session_log)


def clear_session_log():
    """Clear the session log."""
    with _session_lock:
        _session_log.clear()


def log_error(error, context=None):
    """Log an error event to the session buffer with sanitized traceback."""
    tb_str = tb_module.format_exc()
    entry = {
        "ts": datetime.now().isoformat(),
        "action": "error",
        "detail": sanitize_string(str(error))[:300],
    }
    if context:
        entry["context"] = str(context)[:100]
    if tb_str and tb_str.strip() != "NoneType: None":
        lines = tb_str.strip().split('\n')[-5:]
        entry["traceback"] = [sanitize_string(line) for line in lines]

    with _session_lock:
        _session_log.append(entry)


def get_system_info(app_version):
    """Collect non-sensitive system info for feedback context."""
    return {
        "app_version": app_version,
        "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "os": platform.system(),
        "os_version": platform.release(),
        "arch": platform.machine(),
    }


def store_feedback(feedback_data):
    """Store feedback entry locally in feedback.json.

    Args:
        feedback_data: dict with category, description, and optional metadata

    Returns:
        dict with success status and feedback_id
    """
    try:
        entries = []
        if FEEDBACK_PATH.exists():
            try:
                with open(FEEDBACK_PATH, 'r') as f:
                    entries = json.load(f)
            except (json.JSONDecodeError, IOError):
                entries = []

        entries.append(feedback_data)
        # Keep bounded
        entries = entries[-MAX_FEEDBACK_ENTRIES:]

        with open(FEEDBACK_PATH, 'w') as f:
            json.dump(entries, f, indent=2)

        feedback_id = feedback_data.get("feedback_id", "unknown")
        logger.info(f"Feedback stored locally: {feedback_id}")
        return {"success": True, "feedback_id": feedback_id}

    except Exception as e:
        logger.error(f"Failed to store feedback: {e}")
        return {"success": False, "error": str(e)}


def get_stored_feedback():
    """Retrieve all locally stored feedback entries."""
    if FEEDBACK_PATH.exists():
        try:
            with open(FEEDBACK_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return []
