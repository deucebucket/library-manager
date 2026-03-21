"""Custom Layer Builder API routes (Issue #186).

Flask Blueprint providing CRUD and test endpoints for custom HTTP API layers.
These layers let users add their own book metadata sources without writing code.
"""
import json
import logging
import re
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
