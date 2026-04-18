"""PipelineOrchestrator -- configurable replacement for hardcoded process_all_queue.

This module provides a PipelineOrchestrator that executes processing layers
in the order defined by the LayerRegistry and config, using LayerAdapter
objects to delegate to the existing (battle-tested) layer functions.

The orchestrator handles:
    - Layer ordering (from registry + config pipeline_order)
    - Enable/disable checks (from config enable keys)
    - Circuit breaker waits (per-layer, checking _worker_running)
    - Batch loops with configurable delays
    - Rate limit handling (for AI verify layer)
    - Disabled-layer fallback (advancing stuck items)
    - Stuck item recovery
    - Worker stop checks
    - Status updates and logging

Feature flag: controlled by config key ``use_modular_pipeline`` (default False).
When False, the existing process_all_queue runs unchanged. When True, the
orchestrator takes over.
"""

import logging
import time
from typing import Callable, Dict, List, Optional, Tuple

from library_manager.pipeline.adapters import LayerAdapter
from library_manager.pipeline.registry import LayerRegistry

logger = logging.getLogger(__name__)

# Import worker state -- needed for stop checks and status updates
from library_manager.worker import (
    _processing_status,
    update_processing_status,
    LAYER_NAMES,
)


# Layer display numbers for status bar (maps layer_id to legacy layer number)
_LAYER_DISPLAY_NUMBERS = {
    'audio_id': 1,
    'audio_credits': 2,
    'sl_requeue': 2,  # Runs between L2 and L3, show as L2
    'api_lookup': 3,
    'ai_verify': 4,
}

# Delay between batches per layer (seconds)
_BATCH_DELAYS = {
    'audio_id': 2,       # Audio processing needs more time
    'audio_credits': 2,  # Audio processing needs more time
    'sl_requeue': 0,     # Single pass, no batch loop
    'api_lookup': 0.5,   # APIs are fast
    'ai_verify': None,   # Uses rate-limit-based delay (calculated at runtime)
}

# Status messages shown when each layer starts
_LAYER_STATUS_MESSAGES = {
    'audio_id': "Transcribing audio intro via Skaldleita...",
    'audio_credits': "Sending audio clip to Gemini AI...",
    'sl_requeue': "Checking SL requeue verifications...",
    'api_lookup': "Looking up metadata from Skaldleita/Audnexus...",
    'ai_verify': "Verifying identification with AI...",
}

_LAYER_ACTIVITY_MESSAGES = {
    'audio_id': "Started audio identification",
    'audio_credits': "Started AI audio analysis",
    'sl_requeue': "Re-verifying books after nightly merge",
    'api_lookup': "Started API metadata lookup",
    'ai_verify': "Started AI verification of folder names",
}

# Log banner messages
_LAYER_LOG_BANNERS = {
    'audio_id': "LAYER 1: Audio Transcription + AI Parsing",
    'audio_credits': "LAYER 2: AI Audio Clip Analysis (for unclear L1 results)",
    'sl_requeue': "SL REQUEUE CHECK: Re-verifying pending books after nightly merge",
    'api_lookup': "LAYER 3: API Enrichment (adding metadata to identified books)",
    'ai_verify': "LAYER 4: Folder Name Fallback (last resort, low confidence)",
}


class PipelineOrchestrator:
    """Executes processing layers in configured order using adapter pattern.

    The orchestrator replaces the hardcoded layer sequence in
    worker.process_all_queue with a configurable, registry-driven pipeline.
    Each layer runs through its adapter, which delegates to the existing
    layer function with all its battle-tested logic intact.

    Args:
        registry: LayerRegistry defining available layers and ordering.
        adapters: List of LayerAdapter instances (one per layer).
        config: Application configuration dict.
        deps: Dictionary of injected dependencies passed to each adapter.
            Must include: get_db, load_config, is_circuit_open,
            get_circuit_breaker, check_rate_limit, and the layer wrapper
            functions (process_layer_1_audio, process_layer_1_api, etc.).
    """

    def __init__(
        self,
        registry: LayerRegistry,
        adapters: List[LayerAdapter],
        config: Dict,
        deps: Dict,
    ):
        self.registry = registry
        self.adapters = {a.layer_id: a for a in adapters}
        self.config = config
        self.deps = deps

    def run_pipeline(self) -> Tuple[int, int]:
        """Execute all enabled layers in configured order.

        This is the main entry point, replacing the hardcoded sequence in
        process_all_queue. It handles:
            - Queue empty check
            - Stuck item cleanup
            - Disabled layer fallback (advancing items past disabled layers)
            - Per-layer batch loops with circuit breaker waits
            - Status bar updates
            - Final status reset

        Returns:
            Tuple of (total_processed, total_fixed).
        """
        global _processing_status
        import library_manager.worker as _worker_mod

        get_db = self.deps['get_db']

        # Check if queue has items
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) as count FROM queue')
        total = c.fetchone()['count']
        conn.close()

        if total == 0:
            logger.info("Queue is empty, nothing to process")
            return 0, 0

        # Calculate delay for rate-limited layers
        user_max = self.config.get('max_requests_per_hour', 30)
        max_per_hour = max(10, min(user_max, 500))
        min_delay = max(2, 3600 // max_per_hour)
        logger.info(f"Rate limit: {max_per_hour}/hour, delay between batches: {min_delay}s")

        # Initialize processing status
        _processing_status.update({
            "active": True,
            "processed": 0,
            "total": total,
            "current": "Starting processing...",
            "current_book": "",
            "current_author": "",
            "errors": [],
            "layer": 1,
            "layer_name": LAYER_NAMES[1],
            "queue_remaining": total,
            "last_activity": f"Starting processing of {total} items",
            "last_activity_time": time.time(),
        })
        logger.info(f"=== STARTING AUDIO-FIRST PROCESSING: {total} items in queue ===")

        # Issue #62: Clean up stuck queue items
        conn = get_db()
        c = conn.cursor()
        c.execute('''DELETE FROM queue WHERE book_id IN (
                        SELECT b.id FROM books b WHERE b.status IN ('needs_attention', 'verified', 'fixed')
                     )''')
        cleaned = c.rowcount
        if cleaned > 0:
            logger.info(f"Cleaned {cleaned} stuck items from queue (already needs_attention/verified/fixed)")
            total -= cleaned
        conn.commit()
        conn.close()

        # Advance items stuck at Layer 2 if audio analysis is disabled
        if not self.config.get('enable_audio_analysis', False):
            self._advance_stuck_layer2_items()

        total_processed = 0
        total_fixed = 0

        # Get layers in configured order
        ordered_layers = self.registry.get_ordered_layers(self.config)

        for layer_info in ordered_layers:
            # Check worker stop
            if not _worker_mod._worker_running:
                logger.info("Worker stop requested, breaking pipeline")
                break

            adapter = self.adapters.get(layer_info.layer_id)
            if not adapter:
                logger.debug(f"No adapter for layer '{layer_info.layer_id}', skipping")
                continue

            # Check if layer is enabled
            if not self.registry.is_enabled(layer_info.layer_id, self.config):
                self._handle_disabled_layer(layer_info)
                continue

            # Run the layer
            layer_processed, layer_fixed = self._run_layer(
                layer_info, adapter, min_delay
            )
            total_processed += layer_processed
            total_fixed += layer_fixed

        # Reset status to idle
        _processing_status.update({
            "active": False,
            "layer": 0,
            "layer_name": "Idle",
            "current": "Processing complete",
            "current_book": "",
            "current_author": "",
            "queue_remaining": 0,
            "last_activity": f"Completed: {total_processed} processed, {total_fixed} fixed",
            "last_activity_time": time.time(),
        })
        logger.info(f"=== LAYERED PROCESSING COMPLETE: {total_processed} processed, {total_fixed} fixed ===")
        return total_processed, total_fixed

    def _run_layer(
        self,
        layer_info,
        adapter: LayerAdapter,
        min_delay: int,
    ) -> Tuple[int, int]:
        """Run a single layer with its batch loop and circuit breaker logic.

        Handles:
            - Status bar updates before starting
            - Circuit breaker waits (for layers that support them)
            - Batch loop with per-layer delay
            - Special rate-limit handling for ai_verify
            - Worker stop checks between iterations

        Args:
            layer_info: LayerInfo describing this layer.
            adapter: The LayerAdapter to execute.
            min_delay: Rate-limit-based delay for ai_verify layer.

        Returns:
            Tuple of (layer_processed, layer_fixed).
        """
        import library_manager.worker as _worker_mod
        global _processing_status

        layer_id = layer_info.layer_id
        display_num = _LAYER_DISPLAY_NUMBERS.get(layer_id, 0)
        banner = _LAYER_LOG_BANNERS.get(layer_id, f"Layer: {layer_info.layer_name}")
        status_msg = _LAYER_STATUS_MESSAGES.get(layer_id, f"Processing {layer_info.layer_name}...")
        activity_msg = _LAYER_ACTIVITY_MESSAGES.get(layer_id, f"Started {layer_info.layer_name}")

        logger.info(f"=== {banner} ===")
        _processing_status["layer"] = display_num
        _processing_status["layer_name"] = layer_info.layer_name
        _processing_status["current"] = status_msg
        _processing_status["last_activity"] = activity_msg
        _processing_status["last_activity_time"] = time.time()

        # Special case: sl_requeue is single-pass, not a batch loop
        if layer_id == 'sl_requeue':
            return self._run_single_pass(adapter)

        # Special case: ai_verify has its own complex rate-limit loop
        if layer_id == 'ai_verify':
            return self._run_ai_verify_loop(adapter, min_delay)

        # Standard batch loop (audio_id, audio_credits, api_lookup)
        return self._run_batch_loop(layer_info, adapter)

    def _run_batch_loop(
        self,
        layer_info,
        adapter: LayerAdapter,
    ) -> Tuple[int, int]:
        """Standard batch loop: run adapter in a loop until it returns 0.

        Handles circuit breaker waits for layers that support them.

        Args:
            layer_info: LayerInfo for this layer.
            adapter: The adapter to call.

        Returns:
            Tuple of (total_processed, total_resolved).
        """
        import library_manager.worker as _worker_mod
        global _processing_status

        layer_id = layer_info.layer_id
        batch_delay = _BATCH_DELAYS.get(layer_id, 1)
        layer_processed = 0
        layer_resolved = 0

        is_circuit_open = self.deps.get('is_circuit_open')
        get_circuit_breaker = self.deps.get('get_circuit_breaker')

        while True:
            if not _worker_mod._worker_running:
                break

            # Circuit breaker wait for layers that need it
            if layer_info.supports_circuit_breaker and is_circuit_open and get_circuit_breaker:
                should_continue = self._wait_for_circuit_breaker(
                    layer_info, is_circuit_open, get_circuit_breaker
                )
                if should_continue == 'break':
                    break
                if should_continue == 'continue':
                    continue

            processed, resolved = adapter.run(self.config, self.deps)
            if processed == 0:
                break
            layer_processed += processed
            layer_resolved += resolved
            _processing_status["processed"] = _processing_status.get("processed", 0) + processed
            if batch_delay:
                time.sleep(batch_delay)

        logger.info(
            f"{layer_info.layer_name} complete: {layer_processed} items processed, "
            f"{layer_resolved} resolved"
        )
        return layer_processed, layer_resolved

    def _run_single_pass(self, adapter: LayerAdapter) -> Tuple[int, int]:
        """Run a layer once (no batch loop). Used for sl_requeue.

        Args:
            adapter: The adapter to call.

        Returns:
            Tuple of (processed, resolved).
        """
        processed, resolved = adapter.run(self.config, self.deps)
        if processed > 0:
            logger.info(
                f"SL Requeue Check complete: {processed} processed, {resolved} upgraded"
            )
        return processed, resolved

    def _run_ai_verify_loop(
        self,
        adapter: LayerAdapter,
        min_delay: int,
    ) -> Tuple[int, int]:
        """Run AI verify layer with rate limiting and 3-strike exhaustion.

        This replicates the complex Layer 4 logic from process_all_queue:
            - Rate limit checks with exponential backoff
            - processed == -1 means rate-limited (don't count toward exhaustion)
            - 3 consecutive empty batches -> mark remaining as needs_attention
            - Circuit breaker awareness for AI providers
            - Worker stop checks

        Args:
            adapter: The AiVerifyAdapter.
            min_delay: Rate-limit-based delay between batches.

        Returns:
            Tuple of (layer_processed, layer_fixed).
        """
        import library_manager.worker as _worker_mod
        global _processing_status

        get_db = self.deps['get_db']
        load_config = self.deps['load_config']
        check_rate_limit = self.deps['check_rate_limit']
        is_circuit_open = self.deps.get('is_circuit_open')

        batch_num = 0
        rate_limit_hits = 0
        empty_batch_count = 0
        layer_processed = 0
        layer_fixed = 0

        while True:
            if not _worker_mod._worker_running:
                break

            config = load_config()

            allowed, calls_made, max_calls = check_rate_limit(config)
            if not allowed:
                rate_limit_hits += 1
                wait_time = min(300 * rate_limit_hits, 1800)
                logger.info(f"Rate limit reached ({calls_made}/{max_calls}), waiting {wait_time // 60} minutes...")
                _processing_status["current"] = f"Rate limited, waiting {wait_time // 60}min..."
                time.sleep(wait_time)
                continue

            batch_num += 1
            logger.info(f"--- Layer 4 batch {batch_num} (API: {calls_made}/{max_calls}) ---")

            processed, fixed = adapter.run(config, self.deps)

            # Issue #160: processed == -1 means rate-limited
            if processed == -1:
                logger.info("Batch skipped due to rate limiting - not counting toward exhaustion")
                _processing_status["current"] = "Rate limited, waiting for cooldown..."
                _processing_status["last_activity"] = "Waiting for rate limit cooldown"
                _processing_status["last_activity_time"] = time.time()
                time.sleep(30)
                continue

            if processed == 0:
                # Check if AI providers are circuit-broken
                ai_provider = config.get('ai_provider', 'gemini')
                providers_to_check = [ai_provider]
                if ai_provider != 'bookdb':
                    providers_to_check.append('bookdb')

                any_circuit_open = False
                if is_circuit_open:
                    any_circuit_open = any(is_circuit_open(p) for p in providers_to_check)

                if any_circuit_open:
                    broken = ', '.join(p for p in providers_to_check if is_circuit_open(p))
                    logger.info(f"AI providers circuit-broken ({broken}) - waiting for recovery")
                    _processing_status["current"] = "AI provider cooling down, waiting..."
                    _processing_status["last_activity"] = "Waiting for circuit breaker recovery"
                    _processing_status["last_activity_time"] = time.time()
                    time.sleep(30)
                    continue

                conn = get_db()
                c = conn.cursor()
                c.execute('SELECT COUNT(*) as count FROM queue')
                remaining = c.fetchone()['count']
                conn.close()

                if remaining == 0:
                    logger.info("Queue is now empty")
                    break
                else:
                    empty_batch_count += 1
                    logger.warning(f"No items processed but {remaining} remain (attempt {empty_batch_count}/3)")
                    if empty_batch_count >= 3:
                        self._mark_orphaned_items(remaining)
                        break
                    time.sleep(10)
                    continue

            empty_batch_count = 0
            layer_processed += processed
            layer_fixed += fixed
            _processing_status["processed"] = _processing_status.get("processed", 0) + processed
            logger.info(f"Layer 4 Batch {batch_num}: {processed} processed, {fixed} fixed")
            time.sleep(min_delay)

        logger.info(f"Layer 4 complete: {layer_processed} items processed, {layer_fixed} fixed via folder fallback")
        return layer_processed, layer_fixed

    def _wait_for_circuit_breaker(
        self,
        layer_info,
        is_circuit_open: Callable,
        get_circuit_breaker: Callable,
    ) -> Optional[str]:
        """Wait for circuit breakers to close before processing.

        Checks all circuit breaker APIs for the layer. If any are open,
        waits up to 60s at a time, checking _worker_running.

        Args:
            layer_info: LayerInfo with circuit_breaker_apis.
            is_circuit_open: Function to check if a circuit breaker is open.
            get_circuit_breaker: Function to get circuit breaker state dict.

        Returns:
            None if no circuit breaker is open (proceed normally).
            'continue' if we waited and should re-check.
            'break' if worker was stopped during wait.
        """
        import library_manager.worker as _worker_mod
        global _processing_status

        for api_name in layer_info.circuit_breaker_apis:
            if is_circuit_open(api_name):
                cb = get_circuit_breaker(api_name)
                remaining = int(cb.get('circuit_open_until', 0) - time.time())
                if remaining > 0:
                    wait_time = min(remaining, 60)
                    logger.info(
                        f"[{layer_info.layer_name}] {api_name} circuit breaker open, "
                        f"waiting {wait_time}s ({remaining}s total remaining)"
                    )
                    _processing_status["current"] = (
                        f"{layer_info.layer_name}: Waiting for {api_name} ({remaining}s)"
                    )

                    # Sleep in small increments to check worker stop
                    for _ in range(wait_time):
                        if not _worker_mod._worker_running:
                            return 'break'
                        time.sleep(1)

                    return 'continue'

        return None  # No circuit breaker is open

    def _handle_disabled_layer(self, layer_info) -> None:
        """Handle a disabled layer by advancing stuck items past it.

        When audio_credits (Layer 2) is disabled, items stuck at
        verification_layer=2 need to be advanced to Layer 4 so they
        get processed by the folder fallback.

        Args:
            layer_info: LayerInfo for the disabled layer.
        """
        if layer_info.layer_id == 'audio_credits':
            get_db = self.deps['get_db']
            conn = get_db()
            c = conn.cursor()
            c.execute('SELECT id FROM books WHERE verification_layer = 2 AND status = "pending"')
            layer2_books = [row['id'] for row in c.fetchall()]
            if layer2_books:
                c.execute('UPDATE books SET verification_layer = 4 WHERE verification_layer = 2 AND status = "pending"')
                for book_id in layer2_books:
                    c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                    if not c.fetchone():
                        c.execute(
                            'INSERT INTO queue (book_id, reason, priority) VALUES (?, ?, ?)',
                            (book_id, 'layer2_fallback', 5),
                        )
                conn.commit()
                logger.info(f"Layer 2 disabled - advanced {len(layer2_books)} items to Layer 4 (folder fallback)")
            conn.close()

    def _advance_stuck_layer2_items(self) -> None:
        """Advance items stuck at verification_layer=2 when audio analysis is disabled.

        This runs once at pipeline start, before any layers execute.
        Identical to the logic at the top of process_all_queue.
        """
        get_db = self.deps['get_db']
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT id FROM books WHERE verification_layer = 2 AND status = "pending"')
        stuck_books = [row['id'] for row in c.fetchall()]
        if stuck_books:
            c.execute('UPDATE books SET verification_layer = 4 WHERE verification_layer = 2 AND status = "pending"')
            for book_id in stuck_books:
                c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                if not c.fetchone():
                    c.execute(
                        'INSERT INTO queue (book_id, reason, priority) VALUES (?, ?, ?)',
                        (book_id, 'startup_layer2_recovery', 5),
                    )
            conn.commit()
            logger.info(f"Advanced {len(stuck_books)} stuck items from Layer 2 to Layer 4")
        conn.close()

    def _mark_orphaned_items(self, remaining: int) -> None:
        """Mark orphaned queue items as needs_attention after 3-strike exhaustion.

        Replicates the Issue #131 logic from process_all_queue.

        Args:
            remaining: Number of items remaining in queue.
        """
        get_db = self.deps['get_db']
        logger.info(f"Layer 4 cannot process remaining {remaining} items - marking as needs_attention")
        conn = get_db()
        try:
            c = conn.cursor()
            # Issue #168: Increment attempt_count and record last_attempted
            c.execute('''UPDATE books SET status = 'needs_attention',
                            error_message = 'All processing layers exhausted - could not identify this book automatically',
                            attempt_count = COALESCE(attempt_count, 0) + 1,
                            last_attempted = CURRENT_TIMESTAMP,
                            max_layer_reached = MAX(COALESCE(max_layer_reached, 0), COALESCE(verification_layer, 0))
                         WHERE id IN (
                             SELECT q.book_id FROM queue q
                             JOIN books b ON q.book_id = b.id
                             WHERE b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                               AND (b.user_locked IS NULL OR b.user_locked = 0)
                         )''')
            orphaned = c.rowcount
            c.execute('''DELETE FROM queue WHERE book_id IN (
                             SELECT id FROM books WHERE status = 'needs_attention'
                         )''')
            conn.commit()
            if orphaned:
                logger.info(f"Marked {orphaned} orphaned queue items as needs_attention")
        finally:
            conn.close()

    def run_single_layer(self, layer_id: str) -> Tuple[int, int]:
        """Run just one specific layer (for standalone/manual execution).

        Useful for testing or manually triggering a single layer from
        the API without running the full pipeline.

        Args:
            layer_id: The layer_id to run (must be registered and have an adapter).

        Returns:
            Tuple of (processed, resolved).

        Raises:
            KeyError: If layer_id is not registered.
            ValueError: If no adapter exists for the layer.
        """
        layer_info = self.registry.get_layer(layer_id)
        if layer_info is None:
            raise KeyError(f"Unknown layer_id: {layer_id}")

        adapter = self.adapters.get(layer_id)
        if adapter is None:
            raise ValueError(f"No adapter registered for layer: {layer_id}")

        return adapter.run(self.config, self.deps)


__all__ = ['PipelineOrchestrator']
