# Example Plugins

Drop-in Python plugins for Library Manager. Copy a plugin folder to `/data/plugins/` (Docker) or `plugins/` (bare metal) and restart.

## example-logger

A minimal template plugin that logs each book it processes. Use as a starting point for your own plugins.

**Install:**
```bash
cp -r example-logger /data/plugins/
# Restart Library Manager
```

## Creating Your Own Plugin

Each plugin needs two files in its own folder:

### manifest.json

```json
{
  "id": "my-plugin",
  "name": "My Plugin",
  "version": "1.0.0",
  "description": "What it does",
  "type": "layer",
  "entry_point": "layer.py",
  "class_name": "MyPlugin",
  "default_order": 35,
  "requires_config": [],
  "requires_secrets": ["my_api_key"],
  "permissions": {
    "network": ["api.example.com"],
    "database": "read"
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier (alphanumeric, hyphens, underscores) |
| `name` | Yes | Display name |
| `version` | No | Semver version string |
| `description` | No | What the plugin does |
| `type` | Yes | Must be `layer` |
| `entry_point` | Yes | Python file containing the plugin class |
| `class_name` | No | Class name to load (auto-detected if omitted) |
| `default_order` | No | Pipeline position 1-999 (default: 50). Lower = runs earlier |
| `requires_config` | No | Config keys your plugin reads |
| `requires_secrets` | No | Secret keys your plugin needs (stored in secrets.json) |
| `permissions.network` | No | Domains your plugin connects to |
| `permissions.database` | No | `read` or `write` |

### layer.py

```python
from library_manager.plugin_loader import BasePlugin

class MyPlugin(BasePlugin):
    name = "My Plugin"
    description = "What it does"
    version = "1.0.0"

    def setup(self, config, secrets):
        """Called once on startup. Store config/secrets you need."""
        self.api_key = secrets.get('my_api_key')

    def can_process(self, book_data):
        """Return True to process this book, False to skip."""
        return True

    def process(self, book_data):
        """Main logic. Return dict of matched fields, or empty dict for no match.

        book_data contains:
          - current_title: Current title (from path or prior identification)
          - current_author: Current author
          - current_narrator: Current narrator (if known)
          - path: Full filesystem path to the book
          - book_id: Database ID

        Return any of: title, author, narrator, series, series_num, year, language
        """
        return {}

    def teardown(self):
        """Called on shutdown. Clean up resources."""
        pass
```

## Plugin Behavior

- Plugins run with a **30 second timeout** per `process()` call
- Default confidence weight: **60** (configurable via `plugin_configs` in config.json)
- Auto-disabled after **5 consecutive failures** (re-enable from Plugin Health dashboard)
- Plugins never crash the app - exceptions are caught and logged
- Results feed into the book profile system alongside built-in sources

## Configuration

Per-plugin settings in `config.json`:

```json
{
  "plugin_configs": {
    "my-plugin": {
      "timeout": 30,
      "custom_setting": "value"
    }
  }
}
```

Secrets in `secrets.json`:

```json
{
  "my_api_key": "your-key-here"
}
```
