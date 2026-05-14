"""Post-processing hooks for Library Manager (Issue #166, #187).

Runs external commands or webhooks after a book is successfully renamed.
Use cases: m4binder conversion, ABS library scan, Discord notifications, etc.

Issue #187: Extended event system with run_on filtering, body_template support,
standardized event envelope, and emit_event() helper.

This is a self-contained Flask Blueprint - routes, logic, DB schema all in one file.
"""
import json
import logging
import shlex
import shutil
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timezone

import requests as http_requests
from flask import Blueprint, request, jsonify

from library_manager.config import CONFIG_PATH, load_config, load_secrets

logger = logging.getLogger(__name__)

hooks_bp = Blueprint('hooks', __name__)

# Maximum timeout: 1 hour for commands, 60s for webhooks
MAX_COMMAND_TIMEOUT = 3600
MAX_WEBHOOK_TIMEOUT = 60
DEFAULT_COMMAND_TIMEOUT = 300
DEFAULT_WEBHOOK_TIMEOUT = 30

# Template variables available in hooks
TEMPLATE_VARIABLES = [
    'new_path', 'old_path', 'author', 'title', 'narrator',
    'series', 'series_num', 'year', 'media_type',
    'book_id', 'history_id', 'event', 'timestamp',
    # Aliases for clarity
    'new_author', 'new_title', 'new_narrator', 'new_series',
    'new_series_num', 'new_year',
    'old_author', 'old_title',
]

# Supported hook events with descriptions (Issue #187)
HOOK_EVENTS = {
    'scan_started': 'Library scan has started',
    'scan_completed': 'Library scan finished',
    'book_discovered': 'A new book was found during scanning',
    'rename_proposed': 'A rename fix has been proposed for review',
    'rename_applied': 'A rename was successfully applied (formerly "fixed")',
    'rename_rejected': 'A proposed rename was rejected by the user',
    'processing_failed': 'Book processing encountered an error',
    'queue_empty': 'The processing queue is empty',
}

# Backward compat: map old event name to new
_EVENT_ALIASES = {
    'fixed': 'rename_applied',
}


# ============== DATABASE ==============

def init_hook_tables(db_path):
    """Create hook_log table. Called from database.py init_db()."""
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=30)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS hook_log (
        id INTEGER PRIMARY KEY,
        history_id INTEGER,
        book_id INTEGER,
        hook_name TEXT,
        hook_type TEXT DEFAULT 'command',
        success INTEGER DEFAULT 0,
        exit_code INTEGER,
        error TEXT,
        stdout TEXT,
        stderr TEXT,
        duration_ms INTEGER DEFAULT 0,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Add hook_status and hook_error columns to history table (mirrors embed_status/embed_error)
    for col_def in ['hook_status TEXT', 'hook_error TEXT']:
        try:
            c.execute(f'ALTER TABLE history ADD COLUMN {col_def}')
        except Exception:
            pass  # Column already exists

    conn.commit()
    conn.close()


def get_hook_log(get_db, limit=50):
    """Get recent hook execution log entries."""
    conn = get_db()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('''SELECT * FROM hook_log ORDER BY executed_at DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def _log_hook_execution(get_db, history_id, book_id, hook_name, hook_type, result):
    """Write a hook execution result to the hook_log table."""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO hook_log (history_id, book_id, hook_name, hook_type,
                      success, exit_code, error, stdout, stderr, duration_ms)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (history_id, book_id, hook_name, hook_type,
                   1 if result.get('success') else 0,
                   result.get('exit_code'),
                   result.get('error', '')[:2000] if result.get('error') else None,
                   result.get('stdout', '')[:2000] if result.get('stdout') else None,
                   result.get('stderr', '')[:2000] if result.get('stderr') else None,
                   result.get('duration_ms', 0)))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[HOOKS] Failed to log hook execution: {e}")


# ============== TEMPLATE SUBSTITUTION ==============

def build_hook_context(book_id, history_id, old_path, new_path,
                       old_author='', old_title='',
                       new_author='', new_title='',
                       new_narrator='', new_series='', new_series_num='',
                       new_year='', media_type='audiobook', event='fixed'):
    """Build the template variable dict from fix data. All values stringified.

    Note: the ``event`` param still accepts 'fixed' for backward compat; it is
    normalised to 'rename_applied' internally.
    """
    # Normalize legacy event name
    event = _EVENT_ALIASES.get(event, event)

    ctx = {
        'book_id': str(book_id),
        'history_id': str(history_id),
        'old_path': str(old_path),
        'new_path': str(new_path),
        'old_author': str(old_author or ''),
        'old_title': str(old_title or ''),
        'new_author': str(new_author or ''),
        'new_title': str(new_title or ''),
        'new_narrator': str(new_narrator or ''),
        'new_series': str(new_series or ''),
        'new_series_num': str(new_series_num or ''),
        'new_year': str(new_year or ''),
        'media_type': str(media_type or 'audiobook'),
        'event': str(event),
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    # Convenience aliases
    ctx['author'] = ctx['new_author']
    ctx['title'] = ctx['new_title']
    ctx['narrator'] = ctx['new_narrator']
    ctx['series'] = ctx['new_series']
    ctx['series_num'] = ctx['new_series_num']
    ctx['year'] = ctx['new_year']
    return ctx


def build_event_context(event_name, payload=None):
    """Build a generic event context dict for non-rename events.

    For events like scan_started or queue_empty that don't have book-specific data.
    All values are stringified for template substitution.
    """
    event_name = _EVENT_ALIASES.get(event_name, event_name)
    ctx = {
        'event': str(event_name),
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    # Merge any event-specific payload fields
    if payload:
        for key, value in payload.items():
            ctx[key] = str(value) if value is not None else ''
    return ctx


def _build_webhook_envelope(event_name, payload_dict, app_version=None):
    """Wrap a payload dict in the standardized event envelope (Issue #187).

    Envelope format:
        {
            "event": "rename_applied",
            "timestamp": "2026-03-21T14:32:00Z",
            "app_version": "0.9.0-beta.133",
            "payload": { ... }
        }
    """
    return {
        'event': event_name,
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'app_version': app_version or '',
        'payload': payload_dict,
    }


def substitute_template(template, context, shell_escape=False):
    """Replace {{variable}} placeholders in a template string.

    Args:
        template: String with {{variable}} placeholders
        context: Dict of variable values
        shell_escape: If True, use shlex.quote() on values (prevents injection in commands)
    """
    result = template
    for key, value in context.items():
        placeholder = '{{' + key + '}}'
        if placeholder in result:
            safe_value = shlex.quote(value) if shell_escape else value
            result = result.replace(placeholder, safe_value)
    return result


# ============== COMMAND EXECUTION ==============

def execute_command_hook(hook, context):
    """Run a shell command hook via subprocess (never shell=True).

    Returns dict with: success, exit_code, stdout, stderr, duration_ms, error
    """
    command_template = hook.get('command', '')
    if not command_template:
        return {'success': False, 'error': 'No command specified', 'duration_ms': 0}

    # Substitute template variables with shell-escaped values
    resolved_command = substitute_template(command_template, context, shell_escape=True)

    # Parse into args list
    try:
        args = shlex.split(resolved_command)
    except ValueError as e:
        return {'success': False, 'error': f'Invalid command syntax: {e}', 'duration_ms': 0}

    if not args:
        return {'success': False, 'error': 'Empty command after parsing', 'duration_ms': 0}

    # Validate binary exists
    binary = args[0]
    if not shutil.which(binary):
        return {'success': False, 'error': f'Binary not found: {binary}', 'duration_ms': 0}

    # Enforce timeout limits
    timeout = min(hook.get('timeout', DEFAULT_COMMAND_TIMEOUT), MAX_COMMAND_TIMEOUT)

    start = time.monotonic()
    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        duration_ms = int((time.monotonic() - start) * 1000)
        return {
            'success': proc.returncode == 0,
            'exit_code': proc.returncode,
            'stdout': proc.stdout[:5000] if proc.stdout else '',
            'stderr': proc.stderr[:5000] if proc.stderr else '',
            'duration_ms': duration_ms,
        }
    except subprocess.TimeoutExpired:
        duration_ms = int((time.monotonic() - start) * 1000)
        return {
            'success': False,
            'exit_code': -1,
            'error': f'Command timed out after {timeout}s',
            'duration_ms': duration_ms,
        }
    except Exception as e:
        duration_ms = int((time.monotonic() - start) * 1000)
        return {
            'success': False,
            'error': f'Execution error: {e}',
            'duration_ms': duration_ms,
        }


# ============== WEBHOOK EXECUTION ==============

def _resolve_body_template(body_template, context):
    """Recursively substitute {{variable}} placeholders in a body_template structure.

    body_template can be a dict, list, or string. All string values get substitution.
    Returns a new structure with substituted values.
    """
    if isinstance(body_template, str):
        return substitute_template(body_template, context)
    elif isinstance(body_template, dict):
        return {k: _resolve_body_template(v, context) for k, v in body_template.items()}
    elif isinstance(body_template, list):
        return [_resolve_body_template(item, context) for item in body_template]
    else:
        return body_template


def execute_webhook_hook(hook, context, secrets=None, app_version=None):
    """Send an HTTP webhook with context as JSON payload.

    If ``body_template`` is set on the hook, it is used as the payload with
    template variables substituted.  Otherwise, the context is wrapped in the
    standardised event envelope (Issue #187).

    Returns dict with: success, exit_code (HTTP status), stdout (response body), error, duration_ms
    """
    url = hook.get('url', '')
    if not url:
        return {'success': False, 'error': 'No URL specified', 'duration_ms': 0}

    method = hook.get('method', 'POST').upper()
    timeout = min(hook.get('timeout', DEFAULT_WEBHOOK_TIMEOUT), MAX_WEBHOOK_TIMEOUT)

    # Build headers - substitute secrets
    headers = dict(hook.get('headers', {}))
    if secrets:
        secret_context = {'webhook_secret': secrets.get('webhook_secret', '')}
        headers = {k: substitute_template(v, secret_context) for k, v in headers.items()}

    # Substitute template variables in URL (no shell escaping needed for URLs)
    resolved_url = substitute_template(url, context)

    # Build payload - body_template takes priority (Issue #187)
    body_template = hook.get('body_template')
    if body_template:
        payload = _resolve_body_template(body_template, context)
    else:
        # Wrap in standardized envelope
        event_name = context.get('event', 'rename_applied')
        payload = _build_webhook_envelope(event_name, context, app_version)

    start = time.monotonic()
    try:
        if method == 'GET':
            resp = http_requests.get(resolved_url, params=payload if isinstance(payload, dict) else {},
                                     headers=headers, timeout=timeout)
        else:
            headers.setdefault('Content-Type', 'application/json')
            resp = http_requests.post(resolved_url, json=payload, headers=headers, timeout=timeout)

        duration_ms = int((time.monotonic() - start) * 1000)
        return {
            'success': 200 <= resp.status_code < 300,
            'exit_code': resp.status_code,
            'stdout': resp.text[:2000] if resp.text else '',
            'duration_ms': duration_ms,
        }
    except http_requests.Timeout:
        duration_ms = int((time.monotonic() - start) * 1000)
        return {
            'success': False,
            'error': f'Webhook timed out after {timeout}s',
            'duration_ms': duration_ms,
        }
    except Exception as e:
        duration_ms = int((time.monotonic() - start) * 1000)
        return {
            'success': False,
            'error': f'Webhook error: {e}',
            'duration_ms': duration_ms,
        }


# ============== ORCHESTRATOR ==============

def _normalize_run_on(run_on_list):
    """Normalize a hook's run_on list, mapping legacy event names.

    Handles backward compat: 'fixed' -> 'rename_applied'.
    Returns a set for fast membership testing.
    """
    normalized = set()
    for event in run_on_list:
        normalized.add(_EVENT_ALIASES.get(event, event))
    return normalized


def run_hooks(context, config, get_db, secrets=None, app_version=None):
    """Main orchestrator - called from apply_fix() after a successful rename.

    Iterates enabled hooks, routes to correct executor, handles sync/async.
    Hook failure NEVER undoes a successful fix.
    """
    hooks = config.get('post_processing_hooks', [])
    if not hooks:
        return

    event = context.get('event', 'rename_applied')
    # Normalize legacy event name in context
    event = _EVENT_ALIASES.get(event, event)
    history_id = context.get('history_id')
    book_id = context.get('book_id')

    any_error = False
    first_error = None

    for hook in hooks:
        if not hook.get('enabled', True):
            continue

        # Check if this hook should run for this event type (Issue #187)
        # Default to ['rename_applied'] for backward compat (covers old 'fixed' hooks)
        run_on = _normalize_run_on(hook.get('run_on', ['rename_applied']))
        if event not in run_on:
            continue

        hook_name = hook.get('name', 'Unnamed Hook')
        hook_type = hook.get('type', 'command')
        mode = hook.get('mode', 'sync')

        logger.info(f"[HOOKS] Running {hook_type} hook: {hook_name} (mode={mode}, event={event})")

        if mode == 'async':
            # Fire and forget in a background thread
            t = threading.Thread(
                target=_run_single_hook,
                args=(hook, hook_name, hook_type, context, secrets, get_db, history_id, book_id, app_version),
                daemon=True,
            )
            t.start()
        else:
            result = _run_single_hook(hook, hook_name, hook_type, context, secrets, get_db, history_id, book_id, app_version)
            if result and not result.get('success'):
                any_error = True
                if not first_error:
                    first_error = result.get('error') or result.get('stderr', '')[:200]
                if hook.get('on_error') == 'stop':
                    logger.warning(f"[HOOKS] Stopping chain due to on_error=stop for: {hook_name}")
                    break

    # Update history with hook status summary
    if history_id:
        try:
            conn = get_db()
            c = conn.cursor()
            hook_status = 'error' if any_error else 'ok'
            hook_error = first_error[:500] if first_error else None
            c.execute('UPDATE history SET hook_status = ?, hook_error = ? WHERE id = ?',
                      (hook_status, hook_error, int(history_id)))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"[HOOKS] Failed to update history hook status: {e}")


def _run_single_hook(hook, hook_name, hook_type, context, secrets, get_db, history_id, book_id, app_version=None):
    """Execute a single hook and log the result."""
    try:
        if hook_type == 'webhook':
            result = execute_webhook_hook(hook, context, secrets, app_version=app_version)
        else:
            result = execute_command_hook(hook, context)

        if result.get('success'):
            logger.info(f"[HOOKS] {hook_name}: OK ({result.get('duration_ms', 0)}ms)")
        else:
            error_detail = result.get('error') or result.get('stderr', '')[:200]
            logger.warning(f"[HOOKS] {hook_name}: FAILED - {error_detail}")

        _log_hook_execution(get_db, history_id, book_id, hook_name, hook_type, result)
        return result
    except Exception as e:
        logger.error(f"[HOOKS] {hook_name}: Exception - {e}")
        error_result = {'success': False, 'error': str(e), 'duration_ms': 0}
        _log_hook_execution(get_db, history_id, book_id, hook_name, hook_type, error_result)
        return error_result


# ============== EVENT EMISSION (Issue #187) ==============

def emit_event(event_name, context_dict, config, get_db, secrets=None, app_version=None):
    """Emit a hook event, filtering hooks by their run_on list.

    This is the primary entry point for triggering hooks from anywhere in the app.
    It normalizes the event name, ensures the context has the event field set,
    and delegates to run_hooks() which handles filtering, sync/async, and logging.

    Args:
        event_name: One of the HOOK_EVENTS keys (e.g. 'rename_applied', 'scan_started')
        context_dict: Dict of template variables for this event.
                      For rename events, use build_hook_context().
                      For other events, use build_event_context() or pass a plain dict.
        config: The app config dict (must contain 'post_processing_hooks')
        get_db: Database connection factory
        secrets: Optional secrets dict for webhook auth
        app_version: Optional app version string for webhook envelope
    """
    # Normalize legacy event names
    event_name = _EVENT_ALIASES.get(event_name, event_name)

    # Ensure context has the event and timestamp fields
    context_dict['event'] = event_name
    if 'timestamp' not in context_dict:
        context_dict['timestamp'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    logger.debug(f"[HOOKS] Emitting event: {event_name}")
    run_hooks(context_dict, config, get_db, secrets=secrets, app_version=app_version)


# ============== TEST HOOK ==============

def test_hook(hook, secrets=None):
    """Dry run with mock data (Stephen King - The Shining).

    For commands: shows resolved command + checks binary exists.
    For webhooks: actually hits the URL with mock payload.
    """
    mock_context = build_hook_context(
        book_id=999,
        history_id=999,
        old_path='/audiobooks/Unknown/The Shining',
        new_path='/audiobooks/Stephen King/The Shining',
        old_author='Unknown',
        old_title='The Shining',
        new_author='Stephen King',
        new_title='The Shining',
        new_narrator='Steven Weber',
        new_series='',
        new_series_num='',
        new_year='1977',
        media_type='audiobook',
        event='rename_applied',
    )

    hook_type = hook.get('type', 'command')

    if hook_type == 'webhook':
        # Actually send the webhook with mock data
        result = execute_webhook_hook(hook, mock_context, secrets)
        result['resolved_url'] = substitute_template(hook.get('url', ''), mock_context)
        result['payload_preview'] = mock_context
        return result
    else:
        # For commands: show what would run, check binary
        command_template = hook.get('command', '')
        resolved = substitute_template(command_template, mock_context, shell_escape=True)
        try:
            args = shlex.split(resolved)
        except ValueError as e:
            return {'success': False, 'error': f'Invalid command syntax: {e}', 'resolved_command': resolved}

        binary = args[0] if args else ''
        binary_found = bool(shutil.which(binary)) if binary else False

        return {
            'success': binary_found,
            'resolved_command': resolved,
            'binary': binary,
            'binary_found': binary_found,
            'error': f'Binary not found: {binary}' if not binary_found else None,
            'mock_context': mock_context,
        }


# ============== BLUEPRINT ROUTES ==============

@hooks_bp.route('/api/hooks/test', methods=['POST'])
def api_hooks_test():
    """Test a hook config with mock data."""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No hook data provided'}), 400

    secrets = load_secrets()
    result = test_hook(data, secrets)
    return jsonify(result)


@hooks_bp.route('/api/hooks/log')
def api_hooks_log():
    """Get recent hook execution log."""
    from library_manager.database import get_db
    limit = request.args.get('limit', 50, type=int)
    entries = get_hook_log(get_db, limit=min(limit, 200))
    return jsonify({'entries': entries})


@hooks_bp.route('/api/hooks/log/clear', methods=['POST'])
def api_hooks_log_clear():
    """Clear hook execution log."""
    from library_manager.database import get_db
    try:
        conn = get_db()
        conn.execute('DELETE FROM hook_log')
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@hooks_bp.route('/api/hooks/events')
def api_hooks_events():
    """Return available hook events with descriptions (Issue #187)."""
    return jsonify({'events': HOOK_EVENTS})


@hooks_bp.route('/api/hooks/save', methods=['POST'])
def api_hooks_save():
    """Save hooks array to config.json."""
    data = request.get_json()
    if data is None:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    hooks = data.get('hooks', [])

    # Validate each hook has required fields
    for i, hook in enumerate(hooks):
        if not hook.get('name'):
            return jsonify({'success': False, 'error': f'Hook {i+1} is missing a name'}), 400
        hook_type = hook.get('type', 'command')
        if hook_type == 'command' and not hook.get('command'):
            return jsonify({'success': False, 'error': f'Hook "{hook["name"]}" is missing a command'}), 400
        if hook_type == 'webhook' and not hook.get('url'):
            return jsonify({'success': False, 'error': f'Hook "{hook["name"]}" is missing a URL'}), 400

    # Load current config, update hooks, save
    try:
        config = load_config()
        config['post_processing_hooks'] = hooks

        # Remove secrets keys before saving to config.json
        secrets_keys = ['openrouter_api_key', 'gemini_api_key', 'google_books_api_key',
                        'abs_api_token', 'bookdb_api_key', 'webhook_secret']
        config_only = {k: v for k, v in config.items() if k not in secrets_keys}

        with open(CONFIG_PATH, 'w') as f:
            json.dump(config_only, f, indent=2)

        return jsonify({'success': True, 'count': len(hooks)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
