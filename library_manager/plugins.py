"""Custom Layer Builder API routes (Issue #186) + Plugin Health Dashboard (Issue #189).

Flask Blueprint providing CRUD, test, and health monitoring endpoints for custom HTTP
API layers. These layers let users add their own book metadata sources without writing code.
"""
import json
import logging
import re
import sqlite3
import time

import requests as http_requests
from flask import Blueprint, request, jsonify

from library_manager.config import CONFIG_PATH, load_config, load_secrets
from library_manager.pipeline.custom_layer import (
    extract_jsonpath, _build_auth_headers, _substitute_url_template
)

logger = logging.getLogger(__name__)

plugins_bp = Blueprint('plugins', __name__)

# Keys that must never be written to config.json
SECRETS_KEYS = ['openrouter_api_key', 'gemini_api_key', 'google_books_api_key',
                'abs_api_token', 'bookdb_api_key', 'webhook_secret']

# Auto-disable threshold: consecutive failures before a plugin is auto-disabled
AUTO_DISABLE_THRESHOLD = 5


def _slugify(name):
    """Convert a layer name to a safe layer_id slug."""
    slug = name.lower().strip()
    slug = re.sub(r'[^a-z0-9]+', '_', slug)
    slug = slug.strip('_')
    return slug or 'custom_layer'


def _save_config_safe(config):
    """Save config.json, stripping secret keys."""
    config_only = {k: v for k, v in config.items() if k not in SECRETS_KEYS}
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_only, f, indent=2)


# ============== PLUGIN METRICS DATABASE ==============

def init_plugin_metrics_table(db_path):
    """Create plugin_metrics table. Called from database.py init_db()."""
    conn = sqlite3.connect(db_path, timeout=30)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS plugin_metrics (
        id INTEGER PRIMARY KEY,
        plugin_id TEXT NOT NULL,
        timestamp REAL NOT NULL,
        success INTEGER DEFAULT 0,
        duration_ms INTEGER,
        error_message TEXT,
        items_processed INTEGER DEFAULT 0,
        items_resolved INTEGER DEFAULT 0
    )''')

    c.execute('''CREATE INDEX IF NOT EXISTS idx_plugin_metrics_plugin
                 ON plugin_metrics(plugin_id, timestamp)''')

    conn.commit()
    conn.close()


def record_plugin_metric(get_db, plugin_id, success, duration_ms,
                         error_message=None, items_processed=0, items_resolved=0):
    """Record a single plugin execution metric.

    Fast INSERT only - no aggregation on write path.
    Also checks for consecutive failures and triggers auto-disable if needed.

    Args:
        get_db: Callable that returns a database connection
        plugin_id: The custom layer's layer_id
        success: Whether the run succeeded (bool)
        duration_ms: Execution time in milliseconds
        error_message: Error text if failed (truncated to 1000 chars)
        items_processed: Number of items processed this run
        items_resolved: Number of items resolved this run

    Returns:
        True if the plugin was auto-disabled due to consecutive failures
    """
    auto_disabled = False
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO plugin_metrics
                     (plugin_id, timestamp, success, duration_ms, error_message,
                      items_processed, items_resolved)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (plugin_id, time.time(), 1 if success else 0, duration_ms,
                   (error_message or '')[:1000] if error_message else None,
                   items_processed, items_resolved))
        conn.commit()

        # Check for consecutive failures if this run failed
        if not success:
            c.execute('''SELECT success FROM plugin_metrics
                         WHERE plugin_id = ?
                         ORDER BY timestamp DESC
                         LIMIT ?''', (plugin_id, AUTO_DISABLE_THRESHOLD))
            recent = c.fetchall()
            if (len(recent) >= AUTO_DISABLE_THRESHOLD and
                    all(row[0] == 0 for row in recent)):
                # Auto-disable the plugin
                auto_disabled = _auto_disable_plugin(plugin_id)

        conn.close()
    except Exception as e:
        logger.error(f"[PLUGINS] Failed to record metric for {plugin_id}: {e}")

    return auto_disabled


def _auto_disable_plugin(plugin_id):
    """Auto-disable a plugin after consecutive failures.

    Sets auto_disabled: true in the layer config and saves to config.json.

    Returns:
        True if the plugin was disabled, False if not found or already disabled
    """
    try:
        config = load_config()
        custom_layers = config.get('custom_layers', [])

        for layer in custom_layers:
            if layer.get('layer_id') == plugin_id:
                if layer.get('auto_disabled'):
                    return False  # Already disabled
                layer['auto_disabled'] = True
                layer['enabled'] = False
                config['custom_layers'] = custom_layers
                _save_config_safe(config)
                logger.warning(
                    f"[PLUGINS] Auto-disabled plugin '{plugin_id}' after "
                    f"{AUTO_DISABLE_THRESHOLD} consecutive failures"
                )
                return True

        return False
    except Exception as e:
        logger.error(f"[PLUGINS] Failed to auto-disable {plugin_id}: {e}")
        return False


# ============== CRUD ROUTES ==============

@plugins_bp.route('/api/plugins/layers')
def api_plugins_list():
    """List all custom layers from config."""
    config = load_config()
    layers = config.get('custom_layers', [])
    return jsonify({'success': True, 'layers': layers})


@plugins_bp.route('/api/plugins/save-layer', methods=['POST'])
def api_plugins_save():
    """Save or update a custom layer in config.json."""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    layer = data.get('layer', {})
    if not layer.get('layer_name'):
        return jsonify({'success': False, 'error': 'Layer name is required'}), 400
    if not layer.get('url_template'):
        return jsonify({'success': False, 'error': 'URL template is required'}), 400

    # Auto-generate layer_id from name if not provided
    if not layer.get('layer_id'):
        layer['layer_id'] = _slugify(layer['layer_name'])

    # Ensure defaults
    layer.setdefault('enabled', True)
    layer.setdefault('method', 'GET')
    layer.setdefault('timeout', 10)
    layer.setdefault('source_weight', 55)
    layer.setdefault('order', 35)
    layer.setdefault('on_error', 'skip')
    layer.setdefault('response_mapping', {})
    layer.setdefault('request_fields', ['title', 'author'])
    layer.setdefault('circuit_breaker', {'max_failures': 3, 'cooldown': 300})

    # Clamp timeout
    layer['timeout'] = max(1, min(int(layer.get('timeout', 10)), 60))
    layer['source_weight'] = max(0, min(int(layer.get('source_weight', 55)), 100))

    try:
        config = load_config()
        custom_layers = config.get('custom_layers', [])

        # Check if updating existing layer
        existing_idx = None
        for i, existing in enumerate(custom_layers):
            if existing.get('layer_id') == layer['layer_id']:
                existing_idx = i
                break

        if existing_idx is not None:
            custom_layers[existing_idx] = layer
        else:
            custom_layers.append(layer)

        config['custom_layers'] = custom_layers
        _save_config_safe(config)

        return jsonify({'success': True, 'layer_id': layer['layer_id']})
    except Exception as e:
        logger.error(f"[PLUGINS] Failed to save layer: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@plugins_bp.route('/api/plugins/layer/<layer_id>', methods=['DELETE'])
def api_plugins_delete(layer_id):
    """Remove a custom layer from config.json."""
    try:
        config = load_config()
        custom_layers = config.get('custom_layers', [])
        original_count = len(custom_layers)

        custom_layers = [l for l in custom_layers if l.get('layer_id') != layer_id]

        if len(custom_layers) == original_count:
            return jsonify({'success': False, 'error': f'Layer "{layer_id}" not found'}), 404

        config['custom_layers'] = custom_layers
        _save_config_safe(config)

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"[PLUGINS] Failed to delete layer {layer_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@plugins_bp.route('/api/plugins/toggle-layer', methods=['POST'])
def api_plugins_toggle():
    """Toggle a custom layer's enabled state."""
    data = request.get_json()
    if not data or not data.get('layer_id'):
        return jsonify({'success': False, 'error': 'No layer_id provided'}), 400

    layer_id = data['layer_id']
    enabled = bool(data.get('enabled', True))

    try:
        config = load_config()
        custom_layers = config.get('custom_layers', [])

        found = False
        for layer in custom_layers:
            if layer.get('layer_id') == layer_id:
                layer['enabled'] = enabled
                found = True
                break

        if not found:
            return jsonify({'success': False, 'error': f'Layer "{layer_id}" not found'}), 404

        config['custom_layers'] = custom_layers
        _save_config_safe(config)

        return jsonify({'success': True, 'enabled': enabled})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@plugins_bp.route('/api/plugins/test-layer', methods=['POST'])
def api_plugins_test():
    """Test a custom layer config by making the actual API call.

    Receives a layer config and test book data (title/author),
    makes the HTTP request server-side, and returns the results.
    """
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    layer = data.get('layer', {})
    test_title = data.get('test_title', 'The Final Empire')
    test_author = data.get('test_author', 'Brandon Sanderson')

    url_template = layer.get('url_template', '')
    if not url_template:
        return jsonify({'success': False, 'error': 'No URL template provided'}), 400

    method = layer.get('method', 'GET').upper()
    timeout = max(1, min(int(layer.get('timeout', 10)), 60))

    # Build template context from test data
    context = {
        'title': test_title,
        'author': test_author,
        'narrator': '',
        'path': '/test/path',
        'isbn': '',
    }

    # Substitute URL template
    url = _substitute_url_template(url_template, context)

    # Build auth headers
    secrets = load_secrets()
    auth_config = layer.get('auth')
    auth_headers = _build_auth_headers(auth_config, secrets)

    headers = dict(auth_headers)
    headers['Accept'] = 'application/json'

    result = {
        'success': True,
        'url': url,
        'method': method,
        'status_code': None,
        'response_time_ms': None,
        'mapped_fields': {},
        'raw_response': None,
        'error': None,
    }

    start = time.monotonic()
    try:
        if method == 'POST':
            body = {f: context.get(f, '') for f in layer.get('request_fields', ['title', 'author'])}
            headers.setdefault('Content-Type', 'application/json')
            resp = http_requests.post(url, json=body, headers=headers, timeout=timeout)
        else:
            resp = http_requests.get(url, headers=headers, timeout=timeout)

        duration_ms = int((time.monotonic() - start) * 1000)
        result['status_code'] = resp.status_code
        result['response_time_ms'] = duration_ms

        # Try to parse JSON response
        try:
            resp_data = resp.json()
            # Truncate raw response for display (max 2000 chars)
            raw_str = json.dumps(resp_data, indent=2)
            result['raw_response'] = raw_str[:2000] + ('...' if len(raw_str) > 2000 else '')

            # Apply response mappings
            response_mapping = layer.get('response_mapping', {})
            for field_name, jsonpath in response_mapping.items():
                value = extract_jsonpath(resp_data, jsonpath)
                if value is not None:
                    result['mapped_fields'][field_name] = str(value)
                else:
                    result['mapped_fields'][field_name] = None

        except (ValueError, json.JSONDecodeError):
            result['raw_response'] = resp.text[:500]
            result['error'] = 'Response is not valid JSON'

        if not (200 <= resp.status_code < 300):
            result['success'] = False
            result['error'] = f'HTTP {resp.status_code}'

    except http_requests.Timeout:
        duration_ms = int((time.monotonic() - start) * 1000)
        result['success'] = False
        result['response_time_ms'] = duration_ms
        result['error'] = f'Request timed out after {timeout}s'

    except http_requests.ConnectionError as e:
        duration_ms = int((time.monotonic() - start) * 1000)
        result['success'] = False
        result['response_time_ms'] = duration_ms
        result['error'] = f'Connection error: {str(e)[:200]}'

    except Exception as e:
        duration_ms = int((time.monotonic() - start) * 1000)
        result['success'] = False
        result['response_time_ms'] = duration_ms
        result['error'] = f'Error: {str(e)[:200]}'

    return jsonify(result)


# ============== HEALTH DASHBOARD API (Issue #189) ==============

@plugins_bp.route('/api/plugins/health')
def api_plugins_health():
    """Return aggregated health stats for all custom plugins.

    For each plugin returns:
    - success_rate (from last 50 runs)
    - avg_duration_ms
    - last_run timestamp
    - total_processed, total_resolved
    - status: active / errored / auto-disabled
    - recent_errors (last 5 error messages)
    """
    from library_manager.database import get_db

    config = load_config()
    custom_layers = config.get('custom_layers', [])

    if not custom_layers:
        return jsonify({'success': True, 'plugins': []})

    try:
        conn = get_db()
        c = conn.cursor()
        plugins_health = []

        for layer in custom_layers:
            plugin_id = layer.get('layer_id', '')
            if not plugin_id:
                continue

            # Get last 50 runs for success rate
            c.execute('''SELECT success, duration_ms, timestamp, error_message,
                                items_processed, items_resolved
                         FROM plugin_metrics
                         WHERE plugin_id = ?
                         ORDER BY timestamp DESC
                         LIMIT 50''', (plugin_id,))
            recent_rows = c.fetchall()

            if not recent_rows:
                plugins_health.append({
                    'plugin_id': plugin_id,
                    'plugin_name': layer.get('layer_name', plugin_id),
                    'enabled': layer.get('enabled', True),
                    'auto_disabled': layer.get('auto_disabled', False),
                    'status': 'no_data',
                    'success_rate': None,
                    'avg_duration_ms': None,
                    'last_run': None,
                    'total_processed': 0,
                    'total_resolved': 0,
                    'recent_errors': [],
                })
                continue

            total_runs = len(recent_rows)
            successes = sum(1 for r in recent_rows if r[0])
            success_rate = round(successes / total_runs * 100, 1) if total_runs else 0

            durations = [r[1] for r in recent_rows if r[1] is not None]
            avg_duration = int(sum(durations) / len(durations)) if durations else 0

            last_run = recent_rows[0][2] if recent_rows else None

            total_processed = sum(r[4] or 0 for r in recent_rows)
            total_resolved = sum(r[5] or 0 for r in recent_rows)

            # Recent errors (last 5 non-null)
            recent_errors = []
            for r in recent_rows:
                if r[3] and len(recent_errors) < 5:
                    recent_errors.append({
                        'message': r[3],
                        'timestamp': r[2],
                    })

            # Determine status
            auto_disabled = layer.get('auto_disabled', False)
            enabled = layer.get('enabled', True)
            if auto_disabled:
                status = 'auto-disabled'
            elif not enabled:
                status = 'disabled'
            elif success_rate < 50:
                status = 'errored'
            else:
                status = 'active'

            plugins_health.append({
                'plugin_id': plugin_id,
                'plugin_name': layer.get('layer_name', plugin_id),
                'enabled': enabled,
                'auto_disabled': auto_disabled,
                'status': status,
                'success_rate': success_rate,
                'avg_duration_ms': avg_duration,
                'last_run': last_run,
                'total_processed': total_processed,
                'total_resolved': total_resolved,
                'recent_errors': recent_errors,
            })

        conn.close()
        return jsonify({'success': True, 'plugins': plugins_health})

    except Exception as e:
        logger.error(f"[PLUGINS] Health check error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@plugins_bp.route('/api/plugins/health/<plugin_id>/logs')
def api_plugins_health_logs(plugin_id):
    """Return last 20 metric entries for a specific plugin."""
    from library_manager.database import get_db

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''SELECT id, plugin_id, timestamp, success, duration_ms,
                            error_message, items_processed, items_resolved
                     FROM plugin_metrics
                     WHERE plugin_id = ?
                     ORDER BY timestamp DESC
                     LIMIT 20''', (plugin_id,))
        rows = c.fetchall()
        conn.close()

        entries = []
        for r in rows:
            entries.append({
                'id': r[0],
                'plugin_id': r[1],
                'timestamp': r[2],
                'success': bool(r[3]),
                'duration_ms': r[4],
                'error_message': r[5],
                'items_processed': r[6],
                'items_resolved': r[7],
            })

        return jsonify({'success': True, 'entries': entries})

    except Exception as e:
        logger.error(f"[PLUGINS] Health logs error for {plugin_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@plugins_bp.route('/api/plugins/health/<plugin_id>/reset', methods=['POST'])
def api_plugins_health_reset(plugin_id):
    """Reset failure count and re-enable an auto-disabled plugin.

    Clears the auto_disabled flag in config and removes recent failure
    metrics so the consecutive failure counter starts fresh.
    """
    try:
        config = load_config()
        custom_layers = config.get('custom_layers', [])

        found = False
        for layer in custom_layers:
            if layer.get('layer_id') == plugin_id:
                layer['auto_disabled'] = False
                layer['enabled'] = True
                found = True
                break

        if not found:
            return jsonify({'success': False, 'error': f'Plugin "{plugin_id}" not found'}), 404

        config['custom_layers'] = custom_layers
        _save_config_safe(config)

        logger.info(f"[PLUGINS] Reset and re-enabled plugin '{plugin_id}'")
        return jsonify({'success': True, 'enabled': True})

    except Exception as e:
        logger.error(f"[PLUGINS] Reset error for {plugin_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
