"""Base class for processing layers in the identification pipeline.

Each layer in the pipeline follows a common pattern:
1. Fetch a batch of items from the database that match layer criteria
2. Process each item (API calls, AI analysis, etc.)
3. Update the database with results
4. Items either resolve (identified) or advance to the next layer
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class LayerAction(Enum):
    """Actions a layer can take after processing an item."""
    RESOLVED = "resolved"      # Item fully identified, remove from queue
    ADVANCE = "advance"        # Move to next layer for further processing
    SKIP = "skip"              # Skip this item (e.g., not applicable for this layer)
    RETRY = "retry"            # Retry later (e.g., rate limited)
    ERROR = "error"            # Processing failed with error


@dataclass
class LayerResult:
    """Result from processing a single item in a layer.

    Attributes:
        action: What to do with this item
        profile_updates: Dict of field updates to apply to book profile
        confidence: Overall confidence score (0-100)
        source: Source identifier for attribution
        message: Human-readable description of what happened
        error: Error message if action is ERROR
        next_layer: Override for which layer to advance to (None = default next)
    """
    action: LayerAction
    profile_updates: Dict[str, Any] = field(default_factory=dict)
    confidence: int = 0
    source: str = ""
    message: str = ""
    error: Optional[str] = None
    next_layer: Optional[int] = None


class ProcessingLayer(ABC):
    """Abstract base class for processing layers.

    Subclasses must implement:
    - layer_number: Which layer this is (0-4)
    - layer_name: Human-readable name
    - can_process(): Check if item is eligible for this layer
    - process_item(): Process a single item

    The base class provides:
    - fetch_batch(): Get items ready for this layer
    - apply_results(): Save results to database
    - run(): Full processing cycle
    """

    def __init__(self, config: Dict, db_getter, status_updater=None):
        """Initialize the layer.

        Args:
            config: App configuration dict
            db_getter: Callable that returns a database connection
            status_updater: Optional callable to update processing status
        """
        self.config = config
        self.get_db = db_getter
        self.update_status = status_updater or (lambda **kw: None)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    @abstractmethod
    def layer_number(self) -> int:
        """The layer number (0-4) for ordering."""
        pass

    @property
    @abstractmethod
    def layer_name(self) -> str:
        """Human-readable layer name."""
        pass

    @property
    def next_layer_number(self) -> int:
        """Default next layer if item doesn't resolve."""
        return self.layer_number + 1

    @property
    def is_enabled(self) -> bool:
        """Check if this layer is enabled in config."""
        return True  # Override in subclasses that have enable flags

    @abstractmethod
    def can_process(self, item: Dict) -> bool:
        """Check if this layer can process the given item.

        Args:
            item: Database row dict with book info

        Returns:
            True if this layer should process the item
        """
        pass

    @abstractmethod
    def process_item(self, item: Dict) -> LayerResult:
        """Process a single item through this layer.

        Args:
            item: Database row dict with book info

        Returns:
            LayerResult with action and any updates
        """
        pass

    def fetch_batch(self, limit: int = 10) -> List[Dict]:
        """Fetch a batch of items ready for this layer.

        Args:
            limit: Maximum items to fetch

        Returns:
            List of item dicts from database
        """
        conn = self.get_db()
        c = conn.cursor()

        # Default query: items at this layer that are pending
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title,
                            b.verification_layer, b.status, b.profile
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.verification_layer = ?
                       AND b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                       AND (b.user_locked IS NULL OR b.user_locked = 0)
                     ORDER BY q.priority, q.added_at
                     LIMIT ?''', (self.layer_number, limit))

        batch = [dict(row) for row in c.fetchall()]
        conn.close()

        return batch

    def apply_result(self, item: Dict, result: LayerResult) -> bool:
        """Apply a processing result to the database.

        Args:
            item: Original item dict
            result: LayerResult from process_item

        Returns:
            True if item was resolved, False otherwise
        """
        import json

        conn = self.get_db()
        c = conn.cursor()

        try:
            if result.action == LayerAction.RESOLVED:
                # Item identified - update book and remove from queue
                profile_json = json.dumps(result.profile_updates) if result.profile_updates else item.get('profile')

                c.execute('''UPDATE books SET
                            status = 'pending_fix',
                            verification_layer = ?,
                            profile = ?,
                            confidence = ?
                            WHERE id = ?''',
                         (self.layer_number, profile_json, result.confidence, item['book_id']))

                c.execute('DELETE FROM queue WHERE id = ?', (item['queue_id'],))
                conn.commit()

                self.logger.info(f"[{self.layer_name}] Resolved: {item.get('current_title', 'Unknown')} ({result.message})")
                return True

            elif result.action == LayerAction.ADVANCE:
                # Move to next layer
                next_layer = result.next_layer if result.next_layer is not None else self.next_layer_number

                c.execute('''UPDATE books SET
                            verification_layer = ?,
                            status = CASE WHEN status = 'needs_attention' THEN 'pending' ELSE status END
                            WHERE id = ?''',
                         (next_layer, item['book_id']))
                conn.commit()

                self.logger.debug(f"[{self.layer_name}] Advancing to layer {next_layer}: {item.get('current_title', 'Unknown')}")
                return False

            elif result.action == LayerAction.ERROR:
                self.logger.error(f"[{self.layer_name}] Error processing {item.get('current_title', 'Unknown')}: {result.error}")
                return False

            else:
                # SKIP or RETRY - no database changes
                return False

        except Exception as e:
            self.logger.error(f"[{self.layer_name}] Failed to apply result: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def run(self, limit: int = 10) -> tuple:
        """Run one processing cycle for this layer.

        Args:
            limit: Maximum items to process

        Returns:
            Tuple of (processed_count, resolved_count)
        """
        if not self.is_enabled:
            self.logger.debug(f"[{self.layer_name}] Layer disabled, skipping")
            return 0, 0

        batch = self.fetch_batch(limit)
        if not batch:
            return 0, 0

        self.logger.info(f"[{self.layer_name}] Processing {len(batch)} items")
        self.update_status(current=f"{self.layer_name}: Processing", layer=self.layer_number)

        processed = 0
        resolved = 0

        for item in batch:
            if not self.can_process(item):
                continue

            try:
                result = self.process_item(item)
                if self.apply_result(item, result):
                    resolved += 1
                processed += 1

            except Exception as e:
                self.logger.error(f"[{self.layer_name}] Exception processing item: {e}", exc_info=True)
                processed += 1

        self.logger.info(f"[{self.layer_name}] Processed {processed}, resolved {resolved}")
        return processed, resolved
