"""Custom HTTP API layers for the processing pipeline.

Allows users to define custom API endpoints that act as processing layers.
Each custom layer makes HTTP requests to external services and maps the
response fields back into the book profile system.

Config schema (in config.json under "custom_layers"):
    {
        "layer_id": "my_bookdb",
        "layer_name": "My Book Database",
        "enabled": true,
        "order": 35,
        "url_template": "https://api.example.com/search?title={{title}}&author={{author}}",
        "method": "GET",
        "timeout": 10,
        "auth": {"type": "bearer", "token_secret_key": "my_bookdb_key"},
        "request_fields": ["title", "author", "narrator", "path"],
        "response_mapping": {
            "title": "$.results[0].title",
            "author": "$.results[0].author_name"
        },
        "source_weight": 55,
        "on_error": "skip",
        "circuit_breaker": {"max_failures": 3, "cooldown": 300}
    }
"""

import json
import logging
import re
import time
from base64 import b64encode
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote as url_quote

import requests as http_requests

from library_manager.config import load_secrets
from library_manager.models.book_profile import BookProfile

logger = logging.getLogger(__name__)

# Default limits
DEFAULT_TIMEOUT = 10
MAX_TIMEOUT = 60
DEFAULT_BATCH_SIZE = 10

# Circuit breaker state: layer_id -> {"failures": int, "cooldown_until": float}
_circuit_breakers = {}


# ============== JSONPATH EXTRACTION ==============

def extract_jsonpath(data: Any, path: str) -> Any:
    """Extract a value from nested data using a simple JSONPath expression.

    Supports:
        $.key              - top-level access
        $.nested.key       - dot-notation nesting
        $.array[0].field   - array index access

    Args:
        data: Parsed JSON data (dict, list, or primitive)
        path: JSONPath expression starting with $

    Returns:
        Extracted value, or None if path doesn't resolve
    """
    if not path or not path.startswith('$'):
        return None

    # Strip the leading $
    remaining = path[1:]
    if not remaining:
        return data

    # Strip leading dot if present
    if remaining.startswith('.'):
        remaining = remaining[1:]

    current = data

    # Tokenize: split on dots but handle array indices like [0]
    # e.g. "results[0].title" -> ["results[0]", "title"]
    tokens = _tokenize_jsonpath(remaining)

    for token in tokens:
        if current is None:
            return None

        # Check for array index: fieldname[N] or just [N]
        match = re.match(r'^([^\[]*)\[(\d+)\]$', token)
        if match:
            field_name = match.group(1)
            index = int(match.group(2))

            # Navigate to field first (if present)
            if field_name:
                if isinstance(current, dict):
                    current = current.get(field_name)
                else:
                    return None

            # Then index into array
            if isinstance(current, (list, tuple)):
                if 0 <= index < len(current):
                    current = current[index]
                else:
                    return None
            else:
                return None
        else:
            # Simple field access
            if isinstance(current, dict):
                current = current.get(token)
            else:
                return None

    return current


def _tokenize_jsonpath(path: str) -> List[str]:
    """Split a JSONPath remainder into tokens, respecting array brackets.

    Examples:
        "results[0].title" -> ["results[0]", "title"]
        "a.b.c" -> ["a", "b", "c"]
        "items[2].nested[0].value" -> ["items[2]", "nested[0]", "value"]
    """
    tokens = []
    current = ''

    i = 0
    while i < len(path):
        ch = path[i]
        if ch == '.':
            if current:
                tokens.append(current)
                current = ''
        elif ch == '[':
            # Consume until closing bracket
            bracket_start = i
            while i < len(path) and path[i] != ']':
                i += 1
            current += path[bracket_start:i + 1]  # include ]
        else:
            current += ch
        i += 1

    if current:
        tokens.append(current)

    return tokens


# ============== CIRCUIT BREAKER ==============

def _check_circuit_breaker(layer_id: str) -> bool:
    """Check if the circuit breaker allows requests for this layer.

    Returns:
        True if requests are allowed, False if circuit is open (cooldown active)
    """
    state = _circuit_breakers.get(layer_id)
    if not state:
        return True

    cooldown_until = state.get('cooldown_until', 0)
    if time.monotonic() < cooldown_until:
        return False

    # Cooldown expired, reset
    if state.get('failures', 0) > 0:
        state['failures'] = 0
    return True


def _record_failure(layer_id: str, max_failures: int, cooldown: int):
    """Record a failure and potentially trip the circuit breaker."""
    if layer_id not in _circuit_breakers:
        _circuit_breakers[layer_id] = {'failures': 0, 'cooldown_until': 0}

    state = _circuit_breakers[layer_id]
    state['failures'] = state.get('failures', 0) + 1

    if state['failures'] >= max_failures:
        state['cooldown_until'] = time.monotonic() + cooldown
        logger.warning(
            f"[CUSTOM:{layer_id}] Circuit breaker tripped after {state['failures']} failures, "
            f"cooldown {cooldown}s"
        )


def _record_success(layer_id: str):
    """Record a success, resetting the failure counter."""
    if layer_id in _circuit_breakers:
        _circuit_breakers[layer_id]['failures'] = 0


def init_circuit_breaker(layer_id: str):
    """Initialize circuit breaker state for a layer."""
    if layer_id not in _circuit_breakers:
        _circuit_breakers[layer_id] = {'failures': 0, 'cooldown_until': 0}


# ============== REQUEST BUILDING ==============

def _build_template_context(item: Dict) -> Dict[str, str]:
    """Build template variable context from a queue/book item.

    Args:
        item: Database row dict with book info

    Returns:
        Dict of template variables (all strings, safe for URL substitution)
    """
    return {
        'title': str(item.get('current_title') or ''),
        'author': str(item.get('current_author') or ''),
        'narrator': str(item.get('narrator') or ''),
        'path': str(item.get('path') or ''),
        'isbn': str(item.get('isbn') or ''),
    }


def _substitute_url_template(template: str, context: Dict[str, str]) -> str:
    """Replace {{variable}} placeholders in URL template with URL-encoded values.

    Args:
        template: URL string with {{variable}} placeholders
        context: Dict of variable values

    Returns:
        URL with placeholders replaced by URL-encoded values
    """
    result = template
    for key, value in context.items():
        placeholder = '{{' + key + '}}'
        if placeholder in result:
            result = result.replace(placeholder, url_quote(str(value), safe=''))
    return result


def _build_auth_headers(auth_config: Optional[Dict], secrets: Dict) -> Dict[str, str]:
    """Build authentication headers based on auth config.

    Supports:
        none         - no auth headers
        bearer       - Authorization: Bearer <token>
        api_key_header - X-API-Key: <token> (or custom header name)
        basic        - Authorization: Basic <base64(user:pass)>

    Args:
        auth_config: Auth configuration dict with type and token_secret_key
        secrets: Loaded secrets dict

    Returns:
        Dict of headers to add to the request
    """
    if not auth_config:
        return {}

    auth_type = auth_config.get('type', 'none')
    if auth_type == 'none':
        return {}

    token_key = auth_config.get('token_secret_key', '')
    token = secrets.get(token_key, '') if token_key else ''

    if auth_type == 'bearer':
        if not token:
            logger.warning(f"Bearer auth configured but secret '{token_key}' is empty")
            return {}
        return {'Authorization': f'Bearer {token}'}

    elif auth_type == 'api_key_header':
        header_name = auth_config.get('header_name', 'X-API-Key')
        if not token:
            logger.warning(f"API key auth configured but secret '{token_key}' is empty")
            return {}
        return {header_name: token}

    elif auth_type == 'basic':
        username = auth_config.get('username', '')
        password = secrets.get(token_key, '') if token_key else ''
        credentials = b64encode(f'{username}:{password}'.encode()).decode()
        return {'Authorization': f'Basic {credentials}'}

    return {}


# ============== CUSTOM API LAYER ==============

class CustomApiLayer:
    """A user-defined HTTP API processing layer.

    Fetches book items from the queue, queries a custom API endpoint,
    and maps the response back into book profile fields.
    """

    def __init__(self, layer_config: Dict, get_db: Callable, secrets_loader: Callable = None):
        """Initialize the custom API layer.

        Args:
            layer_config: Layer configuration dict (see module docstring for schema)
            get_db: Callable that returns a database connection
            secrets_loader: Callable that returns secrets dict (defaults to load_secrets)
        """
        self.layer_config = layer_config
        self.get_db = get_db
        self._load_secrets = secrets_loader or load_secrets

        self.layer_id = layer_config.get('layer_id', 'unknown')
        self.layer_name = layer_config.get('layer_name', f'Custom: {self.layer_id}')
        self.order = layer_config.get('order', 50)
        self.enabled = layer_config.get('enabled', True)

        self.url_template = layer_config.get('url_template', '')
        self.method = layer_config.get('method', 'GET').upper()
        self.timeout = min(layer_config.get('timeout', DEFAULT_TIMEOUT), MAX_TIMEOUT)

        self.auth_config = layer_config.get('auth')
        self.request_fields = layer_config.get('request_fields', ['title', 'author'])
        self.response_mapping = layer_config.get('response_mapping', {})
        self.source_weight = layer_config.get('source_weight', 55)
        self.on_error = layer_config.get('on_error', 'skip')

        cb_config = layer_config.get('circuit_breaker', {})
        self.cb_max_failures = cb_config.get('max_failures', 3)
        self.cb_cooldown = cb_config.get('cooldown', 300)

        self.log_prefix = f"[CUSTOM:{self.layer_id}]"

    def run(self, config: Dict, deps: Optional[Dict] = None) -> Tuple[int, int]:
        """Run one processing cycle for this custom layer.

        Matches the LayerAdapter interface: accepts config and deps,
        returns (processed_count, resolved_count).

        Records metrics after each batch for the plugin health dashboard.

        Args:
            config: App configuration dict
            deps: Optional dependencies dict (unused, for interface compatibility)

        Returns:
            Tuple of (processed_count, resolved_count)
        """
        if not self.enabled:
            logger.debug(f"{self.log_prefix} Layer disabled, skipping")
            return 0, 0

        if not self.url_template:
            logger.warning(f"{self.log_prefix} No url_template configured, skipping")
            return 0, 0

        # Check circuit breaker
        if not _check_circuit_breaker(self.layer_id):
            logger.debug(f"{self.log_prefix} Circuit breaker open, skipping")
            return 0, 0

        # Fetch batch from queue
        batch = self._fetch_batch(config)
        if not batch:
            return 0, 0

        logger.info(f"{self.log_prefix} Processing {len(batch)} items")

        secrets = self._load_secrets()
        auth_headers = _build_auth_headers(self.auth_config, secrets)

        processed = 0
        resolved = 0
        error_message = None
        start_time = time.monotonic()

        for item in batch:
            try:
                result = self._process_item(item, auth_headers)
                if result is not None:
                    if self._apply_result(item, result):
                        resolved += 1
                processed += 1
            except Exception as e:
                logger.error(f"{self.log_prefix} Exception processing item {item.get('book_id')}: {e}",
                             exc_info=True)
                processed += 1
                if not error_message:
                    error_message = str(e)[:500]

        duration_ms = int((time.monotonic() - start_time) * 1000)
        success = error_message is None and processed > 0

        logger.info(f"{self.log_prefix} Processed {processed}, resolved {resolved}")

        # Record metrics for health dashboard (Issue #189)
        try:
            from library_manager.plugins import record_plugin_metric
            was_disabled = record_plugin_metric(
                self.get_db, self.layer_id,
                success=success,
                duration_ms=duration_ms,
                error_message=error_message,
                items_processed=processed,
                items_resolved=resolved
            )
            if was_disabled:
                self.enabled = False
                logger.warning(f"{self.log_prefix} Auto-disabled due to consecutive failures")
        except Exception as e:
            logger.debug(f"{self.log_prefix} Failed to record metric: {e}")

        return processed, resolved

    def _fetch_batch(self, config: Dict) -> List[Dict]:
        """Fetch items from the queue that are at or below this layer's order.

        Uses the same query pattern as base_layer.py but filters by
        verification_layer matching the custom layer order position.

        Args:
            config: App configuration dict

        Returns:
            List of item dicts from database
        """
        batch_size = config.get('batch_size', DEFAULT_BATCH_SIZE)

        conn = self.get_db()
        c = conn.cursor()

        # Custom layers process items at their assigned verification_layer
        # The order field maps to a verification_layer value
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title,
                            b.verification_layer, b.status, b.profile,
                            b.confidence
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.verification_layer = ?
                       AND b.status NOT IN ('verified', 'fixed', 'series_folder',
                                            'multi_book_files', 'needs_attention')
                       AND (b.user_locked IS NULL OR b.user_locked = 0)
                     ORDER BY q.priority, q.added_at
                     LIMIT ?''', (self.order, batch_size))

        batch = [dict(row) for row in c.fetchall()]
        conn.close()

        return batch

    def _process_item(self, item: Dict, auth_headers: Dict) -> Optional[Dict]:
        """Process a single item by calling the external API.

        Args:
            item: Database row dict with book info
            auth_headers: Pre-built authentication headers

        Returns:
            Dict of mapped response fields, or None on error/skip
        """
        # Build template context from item
        context = _build_template_context(item)

        # Check we have at least one required field populated
        has_data = any(context.get(f) for f in self.request_fields)
        if not has_data:
            logger.debug(f"{self.log_prefix} No request data for book_id={item.get('book_id')}, skipping")
            return None

        # Build the request URL
        url = _substitute_url_template(self.url_template, context)

        # Make the HTTP request
        headers = dict(auth_headers)
        headers.setdefault('Accept', 'application/json')

        start = time.monotonic()
        try:
            if self.method == 'POST':
                # POST with JSON body containing the context fields
                body = {f: context.get(f, '') for f in self.request_fields}
                headers.setdefault('Content-Type', 'application/json')
                resp = http_requests.post(url, json=body, headers=headers, timeout=self.timeout)
            else:
                resp = http_requests.get(url, headers=headers, timeout=self.timeout)

            duration_ms = int((time.monotonic() - start) * 1000)
            logger.info(f"{self.log_prefix} {self.method} {url} -> {resp.status_code} ({duration_ms}ms)")

            if not (200 <= resp.status_code < 300):
                _record_failure(self.layer_id, self.cb_max_failures, self.cb_cooldown)
                logger.warning(f"{self.log_prefix} HTTP {resp.status_code} from {url}")
                return None

            _record_success(self.layer_id)

            # Parse response
            try:
                data = resp.json()
            except (ValueError, json.JSONDecodeError):
                logger.warning(f"{self.log_prefix} Non-JSON response from {url}")
                return None

            # Map response fields using JSONPath
            mapped = {}
            for field_name, jsonpath in self.response_mapping.items():
                value = extract_jsonpath(data, jsonpath)
                if value is not None:
                    mapped[field_name] = str(value)

            if not mapped:
                logger.debug(f"{self.log_prefix} No fields mapped from response for book_id={item.get('book_id')}")
                return None

            return mapped

        except http_requests.Timeout:
            duration_ms = int((time.monotonic() - start) * 1000)
            logger.warning(f"{self.log_prefix} Timeout after {duration_ms}ms: {url}")
            _record_failure(self.layer_id, self.cb_max_failures, self.cb_cooldown)
            return None

        except http_requests.ConnectionError as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            logger.warning(f"{self.log_prefix} Connection error ({duration_ms}ms): {e}")
            _record_failure(self.layer_id, self.cb_max_failures, self.cb_cooldown)
            return None

        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            logger.error(f"{self.log_prefix} Request error ({duration_ms}ms): {e}")
            _record_failure(self.layer_id, self.cb_max_failures, self.cb_cooldown)
            return None

    def _apply_result(self, item: Dict, mapped_fields: Dict) -> bool:
        """Apply mapped response fields to the book profile in the database.

        Args:
            item: Original item dict from database
            mapped_fields: Dict of field_name -> value from API response

        Returns:
            True if the item was resolved (profile updated), False otherwise
        """
        # Build a source name for this custom layer
        source_name = f'custom_{self.layer_id}'

        # Load or create profile
        profile = BookProfile()
        if item.get('profile'):
            try:
                existing = json.loads(item['profile']) if isinstance(item['profile'], str) else item['profile']
                profile = BookProfile.from_dict(existing) if hasattr(BookProfile, 'from_dict') else profile
            except (json.JSONDecodeError, TypeError):
                pass

        # Apply mapped fields to profile
        fields_applied = 0
        for field_name, value in mapped_fields.items():
            if not value:
                continue

            if field_name == 'author':
                if profile.add_author(source_name, value, self.source_weight):
                    fields_applied += 1
            elif field_name == 'title':
                if profile.add_title(source_name, value, self.source_weight):
                    fields_applied += 1
            elif field_name == 'narrator':
                profile.narrator.add_source(source_name, value, self.source_weight)
                fields_applied += 1
            elif field_name == 'series':
                profile.series.add_source(source_name, value, self.source_weight)
                fields_applied += 1
            elif field_name == 'series_num':
                profile.series_num.add_source(source_name, value, self.source_weight)
                fields_applied += 1
            elif field_name == 'year':
                profile.year.add_source(source_name, value, self.source_weight)
                fields_applied += 1
            elif field_name == 'language':
                profile.language.add_source(source_name, value, self.source_weight)
                fields_applied += 1

        if fields_applied == 0:
            return False

        # Add this layer to verification history
        if source_name not in profile.verification_layers_used:
            profile.verification_layers_used.append(source_name)

        profile.finalize()

        # Write updated profile to database
        profile_json = json.dumps(profile.to_dict())
        confidence = profile.overall_confidence

        conn = self.get_db()
        c = conn.cursor()

        try:
            # Update the book with new profile data
            # Advance verification_layer past this layer so _fetch_batch
            # (which queries WHERE verification_layer = self.order) won't
            # pick up the same item again next cycle
            next_layer = self.order + 1
            c.execute('''UPDATE books SET
                        profile = ?,
                        confidence = ?,
                        verification_layer = ?,
                        max_layer_reached = MAX(COALESCE(max_layer_reached, 0), ?)
                        WHERE id = ?''',
                     (profile_json, confidence, next_layer, self.order, item['book_id']))
            conn.commit()

            logger.info(
                f"{self.log_prefix} Updated profile for book_id={item['book_id']} "
                f"({fields_applied} fields, confidence={confidence}%)"
            )
            return True

        except Exception as e:
            logger.error(f"{self.log_prefix} Failed to update book {item['book_id']}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()


# ============== REGISTRATION ==============

def register_custom_layers(
    registry: Any,
    config: Dict,
    get_db: Callable
) -> List[CustomApiLayer]:
    """Read custom_layers from config, register each in the LayerRegistry.

    Args:
        registry: LayerRegistry instance (must have .register() method)
        config: App configuration dict (should contain "custom_layers" list)
        get_db: Callable that returns a database connection

    Returns:
        List of instantiated CustomApiLayer adapters
    """
    custom_configs = config.get('custom_layers', [])
    if not custom_configs:
        return []

    adapters = []

    for layer_cfg in custom_configs:
        layer_id = layer_cfg.get('layer_id')
        if not layer_id:
            logger.warning("[CUSTOM] Skipping custom layer with no layer_id")
            continue

        if not layer_cfg.get('enabled', True):
            logger.debug(f"[CUSTOM:{layer_id}] Layer disabled, skipping registration")
            continue

        if not layer_cfg.get('url_template'):
            logger.warning(f"[CUSTOM:{layer_id}] No url_template, skipping registration")
            continue

        # Create the adapter
        adapter = CustomApiLayer(layer_cfg, get_db)

        # Initialize circuit breaker
        init_circuit_breaker(layer_id)

        # Register in the layer registry if it has the expected interface
        order = layer_cfg.get('order', 50)
        layer_name = layer_cfg.get('layer_name', f'Custom: {layer_id}')

        if hasattr(registry, 'register'):
            try:
                # LayerInfo-style registration: pass the info the registry needs
                registry.register(
                    layer_id=layer_id,
                    layer_name=layer_name,
                    order=order,
                    adapter=adapter,
                    layer_type='custom'
                )
                logger.info(f"[CUSTOM:{layer_id}] Registered custom layer '{layer_name}' at order {order}")
            except TypeError:
                # Registry might have a different signature - try simpler registration
                try:
                    registry.register(layer_id, adapter)
                    logger.info(f"[CUSTOM:{layer_id}] Registered custom layer '{layer_name}'")
                except Exception as e:
                    logger.error(f"[CUSTOM:{layer_id}] Failed to register: {e}")
                    continue
        else:
            logger.debug(f"[CUSTOM:{layer_id}] Registry has no register method, adapter created but not registered")

        adapters.append(adapter)

    if adapters:
        logger.info(f"[CUSTOM] Registered {len(adapters)} custom layer(s)")

    return adapters
