"""Example plugin for Library Manager.

This is a minimal plugin that demonstrates the BasePlugin interface.
It logs each book it sees and returns empty results (no modifications).

To use this as a template:
1. Copy this directory to /data/plugins/your-plugin-name/
2. Edit manifest.json with your plugin's metadata
3. Implement process() with your logic
4. Restart Library Manager

The plugin loader will discover and load your plugin automatically.
"""

import logging

# Import BasePlugin from the plugin loader
from library_manager.plugin_loader import BasePlugin

logger = logging.getLogger(__name__)


class ExampleLoggerPlugin(BasePlugin):
    """A simple plugin that logs book information.

    This demonstrates:
    - setup() for one-time initialization
    - can_process() for filtering books
    - process() for the main logic
    - teardown() for cleanup
    """

    name = "Example Logger"
    description = "Logs book data for debugging"
    version = "1.0.0"

    def setup(self, config, secrets):
        """Store config for later use."""
        self.log_level = config.get('log_level', 'info')
        self.books_seen = 0
        logger.info("[ExamplePlugin] Setup complete")

    def can_process(self, book_data):
        """Process all books."""
        return True

    def process(self, book_data):
        """Log the book data and return empty (no changes).

        In a real plugin, you would:
        1. Extract info from book_data (title, author, path, etc.)
        2. Query your data source (API, database, file, etc.)
        3. Return a dict with matched fields

        Example return for a match:
            return {
                'title': 'The Corrected Title',
                'author': 'Correct Author Name',
                'narrator': 'Narrator Name',
            }
        """
        self.books_seen += 1
        title = book_data.get('current_title', 'Unknown')
        author = book_data.get('current_author', 'Unknown')

        logger.info(
            f"[ExamplePlugin] Book #{self.books_seen}: "
            f"'{title}' by {author}"
        )

        # Return empty dict = no changes (this is just a logger)
        return {}

    def teardown(self):
        """Log summary on shutdown."""
        logger.info(
            f"[ExamplePlugin] Shutting down. Saw {self.books_seen} books total."
        )
