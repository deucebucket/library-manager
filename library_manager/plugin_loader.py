"""Drop-in Python plugin system for Library Manager.

Discovers, validates, and loads plugins from a configurable directory
(default: /data/plugins for Docker). Plugins extend the processing
pipeline with custom book identification logic.

Plugin structure:
    /data/plugins/
      my_plugin/
        manifest.json    # metadata, config schema
        layer.py         # class extending BasePlugin

See BasePlugin for the simplified interface plugins should implement.
"""

import copy
import importlib.util
import json
import logging
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from library_manager.config import load_secrets
from library_manager.pipeline.layer_info import LayerInfo

logger = logging.getLogger(__name__)

LOG_PREFIX = "[PLUGIN]"

# Manifest required fields
REQUIRED_MANIFEST_FIELDS = ('id', 'name', 'entry_point')

# Default timeout for plugin process() calls (seconds)
DEFAULT_PLUGIN_TIMEOUT = 30

# Default batch size for plugin processing
DEFAULT_PLUGIN_BATCH_SIZE = 10


# ============== BASE PLUGIN CLASS ==============

class BasePlugin:
    """Simple base class for drop-in plugins.

    Plugins extend this class and implement process() at minimum.
    The plugin loader wraps BasePlugin subclasses in a PluginAdapter
    that makes them compatible with the pipeline's LayerAdapter interface.

    Attributes:
        name: Human-readable plugin name (set from manifest).
        description: What this plugin does (set from manifest).
        version: Plugin version string (set from manifest).
    """

    name = "unnamed"
    description = ""
    version = "0.0.1"

    def setup(self, config: dict, secrets: dict):
        """Called once on load. Store config, create sessions, etc.

        Args:
            config: Plugin-specific configuration from plugin_configs.
            secrets: Full secrets dict from secrets.json.
        """
        pass

    def can_process(self, book_data: dict) -> bool:
        """Return True if this plugin should process this book.

        Args:
            book_data: Dict with book info (current_title, current_author,
                       path, status, profile, etc.)

        Returns:
            True to process this book, False to skip.
        """
        return True

    def process(self, book_data: dict) -> dict:
        """Process a book. Return dict with metadata fields.

        The returned dict can contain any of these keys:
            title, author, narrator, series, series_num, year, language

        Return empty dict or None to skip (no changes).

        Args:
            book_data: Deep copy of book info dict. Safe to modify.

        Returns:
            Dict of metadata fields, or None/empty dict to skip.
        """
        return {}

    def teardown(self):
        """Called on shutdown. Clean up resources."""
        pass


# ============== PLUGIN INFO ==============

@dataclass
class PluginInfo:
    """Metadata about a discovered plugin.

    Populated from manifest.json during discovery.
    """
    plugin_id: str
    name: str
    version: str
    description: str
    plugin_dir: Path
    entry_point: str
    class_name: str = ""
    default_order: int = 35
    plugin_type: str = "layer"
    requires_config: List[str] = field(default_factory=list)
    requires_secrets: List[str] = field(default_factory=list)
    permissions: Dict[str, Any] = field(default_factory=dict)
    manifest: Dict[str, Any] = field(default_factory=dict)

    @property
    def entry_point_path(self) -> Path:
        return self.plugin_dir / self.entry_point


# ============== PLUGIN ADAPTER ==============

class PluginAdapter:
    """Wraps a BasePlugin instance into a LayerAdapter-compatible object.

    Handles:
    - Fetching batch items from the database
    - Deep copying book data before passing to plugins
    - Timeout enforcement via ThreadPoolExecutor
    - Exception isolation (bad plugins never crash the app)
    - Recording metrics via record_plugin_metric()
    """

    def __init__(self, plugin: BasePlugin, plugin_info: PluginInfo,
                 get_db: Callable, timeout: int = DEFAULT_PLUGIN_TIMEOUT):
        """Initialize the plugin adapter.

        Args:
            plugin: Instantiated BasePlugin subclass.
            plugin_info: PluginInfo from manifest discovery.
            get_db: Callable that returns a database connection.
            timeout: Max seconds for a single process() call.
        """
        self.plugin = plugin
        self.plugin_info = plugin_info
        self.get_db = get_db
        self.timeout = timeout
        self.layer_id = f"plugin_{plugin_info.plugin_id}"
        self.log_prefix = f"[PLUGIN:{plugin_info.plugin_id}]"
        self.enabled = True

    def run(self, config: Dict, deps: Optional[Dict] = None) -> Tuple[int, int]:
        """Run one processing cycle for this plugin.

        Matches the LayerAdapter interface: accepts config and deps,
        returns (processed_count, resolved_count).

        Args:
            config: App configuration dict.
            deps: Optional dependencies dict (unused, for interface compat).

        Returns:
            Tuple of (processed_count, resolved_count).
        """
        if not self.enabled:
            logger.debug(f"{self.log_prefix} Plugin disabled, skipping")
            return 0, 0

        # Fetch batch from queue
        batch = self._fetch_batch(config)
        if not batch:
            return 0, 0

        logger.info(f"{self.log_prefix} Processing {len(batch)} items")

        processed = 0
        resolved = 0
        error_message = None
        start_time = time.monotonic()

        for item in batch:
            try:
                result = self._process_item(item)
                if result:
                    if self._apply_result(item, result):
                        resolved += 1
                processed += 1
            except Exception as e:
                logger.error(
                    f"{self.log_prefix} Exception processing item "
                    f"{item.get('book_id')}: {e}", exc_info=True
                )
                processed += 1
                if not error_message:
                    error_message = str(e)[:500]

        duration_ms = int((time.monotonic() - start_time) * 1000)
        success = error_message is None and processed > 0

        logger.info(
            f"{self.log_prefix} Processed {processed}, resolved {resolved} "
            f"({duration_ms}ms)"
        )

        # Record metrics for health dashboard
        try:
            from library_manager.plugins import record_plugin_metric
            was_disabled = record_plugin_metric(
                self.get_db, self.layer_id,
                success=success,
                duration_ms=duration_ms,
                error_message=error_message,
                items_processed=processed,
                items_resolved=resolved,
            )
            if was_disabled:
                self.enabled = False
                logger.warning(
                    f"{self.log_prefix} Auto-disabled due to consecutive failures"
                )
        except Exception as e:
            logger.debug(f"{self.log_prefix} Failed to record metric: {e}")

        return processed, resolved

    def _fetch_batch(self, config: Dict) -> List[Dict]:
        """Fetch items from the queue at this plugin's order position."""
        batch_size = config.get('batch_size', DEFAULT_PLUGIN_BATCH_SIZE)

        conn = self.get_db()
        c = conn.cursor()

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
                     LIMIT ?''', (self.plugin_info.default_order, batch_size))

        batch = [dict(row) for row in c.fetchall()]
        conn.close()

        return batch

    def _process_item(self, item: Dict) -> Optional[Dict]:
        """Process a single item through the plugin with timeout enforcement.

        Deep copies book data before passing to the plugin. Runs the
        plugin's process() method in a thread pool with a timeout.

        Args:
            item: Database row dict with book info.

        Returns:
            Dict of metadata fields from the plugin, or None on error/skip.
        """
        # Deep copy the item so plugins can't mutate our data
        book_data = copy.deepcopy(item)

        # Check if plugin wants to process this book
        try:
            if not self.plugin.can_process(book_data):
                logger.debug(
                    f"{self.log_prefix} Plugin skipped book_id={item.get('book_id')}"
                )
                return None
        except Exception as e:
            logger.warning(
                f"{self.log_prefix} can_process() raised: {e}"
            )
            return None

        # Run process() with timeout enforcement
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.plugin.process, book_data)
                result = future.result(timeout=self.timeout)
        except FuturesTimeoutError:
            logger.warning(
                f"{self.log_prefix} process() timed out after {self.timeout}s "
                f"for book_id={item.get('book_id')}"
            )
            return None
        except Exception as e:
            logger.error(
                f"{self.log_prefix} process() raised: {e}",
                exc_info=True
            )
            return None

        if not result:
            return None

        # Validate result is a dict
        if not isinstance(result, dict):
            logger.warning(
                f"{self.log_prefix} process() returned {type(result).__name__}, "
                f"expected dict"
            )
            return None

        return result

    def _apply_result(self, item: Dict, mapped_fields: Dict) -> bool:
        """Apply plugin results to the book profile in the database.

        Args:
            item: Original item dict from database.
            mapped_fields: Dict of field_name -> value from plugin.

        Returns:
            True if the item was updated, False otherwise.
        """
        from library_manager.models.book_profile import BookProfile

        source_name = f'plugin_{self.plugin_info.plugin_id}'
        source_weight = 60  # Plugin default weight

        # Load or create profile
        profile = BookProfile()
        if item.get('profile'):
            try:
                existing = json.loads(item['profile']) if isinstance(item['profile'], str) else item['profile']
                if hasattr(BookProfile, 'from_dict'):
                    profile = BookProfile.from_dict(existing)
            except (json.JSONDecodeError, TypeError):
                pass

        # Apply fields
        fields_applied = 0
        for field_name, value in mapped_fields.items():
            if not value:
                continue

            if field_name == 'author':
                if profile.add_author(source_name, value, source_weight):
                    fields_applied += 1
            elif field_name == 'title':
                if profile.add_title(source_name, value, source_weight):
                    fields_applied += 1
            elif field_name == 'narrator':
                profile.narrator.add_source(source_name, value, source_weight)
                fields_applied += 1
            elif field_name == 'series':
                profile.series.add_source(source_name, value, source_weight)
                fields_applied += 1
            elif field_name == 'series_num':
                profile.series_num.add_source(source_name, value, source_weight)
                fields_applied += 1
            elif field_name == 'year':
                profile.year.add_source(source_name, value, source_weight)
                fields_applied += 1
            elif field_name == 'language':
                profile.language.add_source(source_name, value, source_weight)
                fields_applied += 1

        if fields_applied == 0:
            return False

        # Add to verification history
        if source_name not in profile.verification_layers_used:
            profile.verification_layers_used.append(source_name)

        profile.finalize()

        # Write to database
        profile_json = json.dumps(profile.to_dict())
        confidence = profile.overall_confidence

        conn = self.get_db()
        c = conn.cursor()

        try:
            next_layer = self.plugin_info.default_order + 1
            c.execute('''UPDATE books SET
                        profile = ?,
                        confidence = ?,
                        verification_layer = ?,
                        max_layer_reached = MAX(COALESCE(max_layer_reached, 0), ?)
                        WHERE id = ?''',
                     (profile_json, confidence, next_layer,
                      self.plugin_info.default_order, item['book_id']))
            conn.commit()

            logger.info(
                f"{self.log_prefix} Updated profile for book_id={item['book_id']} "
                f"({fields_applied} fields, confidence={confidence}%)"
            )
            return True

        except Exception as e:
            logger.error(
                f"{self.log_prefix} Failed to update book {item['book_id']}: {e}"
            )
            conn.rollback()
            return False
        finally:
            conn.close()


# ============== MANIFEST VALIDATION ==============

def _validate_manifest(manifest: Dict, plugin_dir: Path) -> List[str]:
    """Validate a plugin manifest.

    Args:
        manifest: Parsed manifest dict.
        plugin_dir: Path to the plugin directory.

    Returns:
        List of error strings. Empty list means valid.
    """
    errors = []

    for field_name in REQUIRED_MANIFEST_FIELDS:
        if not manifest.get(field_name):
            errors.append(f"Missing required field: {field_name}")

    # Validate entry_point exists
    entry_point = manifest.get('entry_point', '')
    if entry_point:
        entry_path = plugin_dir / entry_point
        if not entry_path.exists():
            errors.append(f"Entry point file not found: {entry_point}")
        elif not entry_path.suffix == '.py':
            errors.append(f"Entry point must be a .py file: {entry_point}")

    # Validate plugin ID format (alphanumeric, hyphens, underscores)
    plugin_id = manifest.get('id', '')
    if plugin_id:
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', plugin_id):
            errors.append(
                f"Invalid plugin id '{plugin_id}': use only letters, numbers, "
                f"hyphens, underscores"
            )

    # Validate type
    plugin_type = manifest.get('type', 'layer')
    if plugin_type not in ('layer',):
        errors.append(f"Unsupported plugin type: {plugin_type}")

    # Validate default_order is reasonable
    order = manifest.get('default_order', 35)
    if not isinstance(order, int) or order < 1 or order > 999:
        errors.append("default_order must be an integer between 1 and 999")

    return errors


# ============== DISCOVERY ==============

def discover_plugins(plugin_dir: Path) -> List[PluginInfo]:
    """Scan a directory for valid plugins.

    Looks for subdirectories containing manifest.json, validates them,
    and returns PluginInfo objects for valid plugins.

    Args:
        plugin_dir: Directory to scan for plugins.

    Returns:
        List of PluginInfo for valid plugins. Invalid plugins are
        logged as warnings and skipped.
    """
    if not plugin_dir.exists():
        logger.info(f"{LOG_PREFIX} Plugin directory does not exist: {plugin_dir}")
        return []

    if not plugin_dir.is_dir():
        logger.warning(f"{LOG_PREFIX} Plugin path is not a directory: {plugin_dir}")
        return []

    plugins = []

    for subdir in sorted(plugin_dir.iterdir()):
        if not subdir.is_dir():
            continue

        # Skip hidden directories and __pycache__
        if subdir.name.startswith('.') or subdir.name == '__pycache__':
            continue

        manifest_path = subdir / 'manifest.json'
        if not manifest_path.exists():
            logger.debug(
                f"{LOG_PREFIX} Skipping {subdir.name}: no manifest.json"
            )
            continue

        # Parse manifest
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(
                f"{LOG_PREFIX} Invalid manifest.json in {subdir.name}: {e}"
            )
            continue

        # Validate manifest
        errors = _validate_manifest(manifest, subdir)
        if errors:
            for err in errors:
                logger.warning(f"{LOG_PREFIX} {subdir.name}: {err}")
            continue

        info = PluginInfo(
            plugin_id=manifest['id'],
            name=manifest['name'],
            version=manifest.get('version', '0.0.1'),
            description=manifest.get('description', ''),
            plugin_dir=subdir,
            entry_point=manifest['entry_point'],
            class_name=manifest.get('class_name', ''),
            default_order=manifest.get('default_order', 35),
            plugin_type=manifest.get('type', 'layer'),
            requires_config=manifest.get('requires_config', []),
            requires_secrets=manifest.get('requires_secrets', []),
            permissions=manifest.get('permissions', {}),
            manifest=manifest,
        )

        logger.info(
            f"{LOG_PREFIX} Discovered plugin: {info.name} v{info.version} "
            f"({info.plugin_id})"
        )
        plugins.append(info)

    if plugins:
        logger.info(f"{LOG_PREFIX} Discovered {len(plugins)} plugin(s)")
    else:
        logger.debug(f"{LOG_PREFIX} No plugins found in {plugin_dir}")

    return plugins


# ============== LOADING ==============

def load_plugin(plugin_info: PluginInfo, config: dict = None,
                secrets: dict = None) -> Optional[BasePlugin]:
    """Load and instantiate a plugin from its PluginInfo.

    Uses importlib to dynamically load the plugin module, finds the
    target class (by class_name or auto-discovery), instantiates it,
    and calls setup().

    Args:
        plugin_info: PluginInfo from discovery.
        config: Plugin-specific config (from plugin_configs).
        secrets: Full secrets dict.

    Returns:
        Instantiated BasePlugin, or None on failure.
    """
    entry_path = plugin_info.entry_point_path
    module_name = f"lm_plugin_{plugin_info.plugin_id}"

    try:
        # Load the module
        spec = importlib.util.spec_from_file_location(module_name, entry_path)
        if spec is None or spec.loader is None:
            logger.error(
                f"{LOG_PREFIX} Failed to create module spec for "
                f"{plugin_info.plugin_id}: {entry_path}"
            )
            return None

        module = importlib.util.module_from_spec(spec)

        # Add to sys.modules so relative imports work within plugins
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            logger.error(
                f"{LOG_PREFIX} Failed to execute module for "
                f"{plugin_info.plugin_id}: {e}"
            )
            # Clean up on failure
            sys.modules.pop(module_name, None)
            return None

        # Find the plugin class
        plugin_class = None

        if plugin_info.class_name:
            # Look up by explicit class name
            plugin_class = getattr(module, plugin_info.class_name, None)
            if plugin_class is None:
                logger.error(
                    f"{LOG_PREFIX} Class '{plugin_info.class_name}' not found "
                    f"in {entry_path}"
                )
                return None
        else:
            # Auto-discover: find first BasePlugin subclass
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type)
                        and issubclass(attr, BasePlugin)
                        and attr is not BasePlugin):
                    plugin_class = attr
                    break

            if plugin_class is None:
                logger.error(
                    f"{LOG_PREFIX} No BasePlugin subclass found in {entry_path}"
                )
                return None

        # Instantiate
        try:
            instance = plugin_class()
        except Exception as e:
            logger.error(
                f"{LOG_PREFIX} Failed to instantiate {plugin_class.__name__} "
                f"for {plugin_info.plugin_id}: {e}"
            )
            return None

        # Set metadata from manifest
        instance.name = plugin_info.name
        instance.description = plugin_info.description
        instance.version = plugin_info.version

        # Call setup
        try:
            instance.setup(config or {}, secrets or {})
        except Exception as e:
            logger.error(
                f"{LOG_PREFIX} setup() failed for {plugin_info.plugin_id}: {e}"
            )
            return None

        logger.info(
            f"{LOG_PREFIX} Loaded plugin: {plugin_info.name} v{plugin_info.version} "
            f"({plugin_class.__name__})"
        )
        return instance

    except Exception as e:
        logger.error(
            f"{LOG_PREFIX} Unexpected error loading {plugin_info.plugin_id}: {e}",
            exc_info=True
        )
        return None


# ============== REGISTRATION ==============

def register_plugins(registry, config: dict, get_db: Callable) -> List[PluginAdapter]:
    """Discover, load, and register all plugins.

    This is the main entry point called from app.py on startup. It:
    1. Reads plugin_dir from config
    2. Discovers plugins in that directory
    3. Loads each plugin (with config and secrets)
    4. Wraps each in a PluginAdapter
    5. Registers each in the LayerRegistry

    Args:
        registry: LayerRegistry instance.
        config: Full app configuration dict.
        get_db: Callable that returns a database connection.

    Returns:
        List of PluginAdapter instances for loaded plugins.
    """
    plugin_dir = Path(config.get('plugin_dir', '/data/plugins'))
    plugin_configs = config.get('plugin_configs', {})

    # Discover
    discovered = discover_plugins(plugin_dir)
    if not discovered:
        return []

    # Load secrets once
    secrets = load_secrets()

    adapters = []

    for info in discovered:
        # Get per-plugin config
        plugin_config = plugin_configs.get(info.plugin_id, {})

        # Check for missing required secrets
        missing_secrets = [
            s for s in info.requires_secrets
            if not secrets.get(s)
        ]
        if missing_secrets:
            logger.warning(
                f"{LOG_PREFIX} {info.plugin_id}: missing required secrets: "
                f"{', '.join(missing_secrets)} -- skipping"
            )
            continue

        # Load the plugin
        instance = load_plugin(info, config=plugin_config, secrets=secrets)
        if instance is None:
            continue

        # Determine timeout from plugin config or default
        timeout = plugin_config.get('timeout', DEFAULT_PLUGIN_TIMEOUT)

        # Wrap in adapter
        adapter = PluginAdapter(instance, info, get_db, timeout=timeout)

        # Register in the layer registry
        layer_id = adapter.layer_id
        try:
            registry.register(LayerInfo(
                layer_id=layer_id,
                layer_name=info.name,
                description=info.description or f"Plugin: {info.name}",
                config_enable_key=f"plugin_{info.plugin_id}_enabled",
                default_order=info.default_order,
                supports_circuit_breaker=False,
            ))
            logger.info(
                f"{LOG_PREFIX} Registered {info.name} as '{layer_id}' "
                f"at order {info.default_order}"
            )
        except ValueError as e:
            # Already registered (duplicate plugin ID)
            logger.warning(f"{LOG_PREFIX} {info.plugin_id}: {e}")
            continue

        adapters.append(adapter)

    if adapters:
        logger.info(f"{LOG_PREFIX} Loaded {len(adapters)} plugin(s)")

    return adapters


def teardown_plugins(adapters: List[PluginAdapter]):
    """Call teardown() on all loaded plugins.

    Called during app shutdown to clean up plugin resources.

    Args:
        adapters: List of PluginAdapter instances.
    """
    for adapter in adapters:
        try:
            adapter.plugin.teardown()
            logger.debug(
                f"{LOG_PREFIX} Teardown complete: {adapter.plugin_info.plugin_id}"
            )
        except Exception as e:
            logger.warning(
                f"{LOG_PREFIX} teardown() failed for "
                f"{adapter.plugin_info.plugin_id}: {e}"
            )
