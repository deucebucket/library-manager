"""Background worker and queue orchestration for Library Manager.

This module manages the background processing threads and orchestrates
the multi-layer book identification pipeline.
"""

import logging
import threading
import time
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Worker state - managed by this module
_worker_thread = None
_worker_running = False
_watch_worker_thread = None
_watch_worker_running = False
_processing_status = {
    "active": False,
    "processed": 0,
    "total": 0,
    "current": "",  # Stage description
    "current_book": "",  # Book title being processed
    "current_author": "",  # Author of current book
    "errors": [],
    "layer": 0,
    "layer_name": "",  # Human-readable layer name
    "queue_remaining": 0,
    "last_activity": "",  # Last significant event
    "last_activity_time": 0,  # Timestamp of last activity
    # NEW: Detailed provider/API tracking
    "current_provider": "",  # e.g., "Skaldleita", "Gemini", "BookDB API"
    "current_step": "",  # e.g., "Transcribing audio...", "Querying database..."
    "provider_chain": [],  # List of providers in order
    "provider_index": 0,  # Current position in chain
    "api_latency_ms": 0,  # Last API call latency
    "confidence": 0,  # Current identification confidence
    "is_free_api": True,  # Whether current API is free (user's data/quota not used)
}

# Layer name mapping for human-readable display
LAYER_NAMES = {
    0: "Idle",
    1: "Audio ID",
    2: "AI Analysis",
    3: "API Lookup",
    4: "AI Verify"
}


def get_processing_status() -> Dict:
    """Get the current processing status."""
    status = _processing_status.copy()
    # Add computed fields
    status["layer_name"] = LAYER_NAMES.get(status.get("layer", 0), "Unknown")
    return status


def update_processing_status(key: str, value) -> None:
    """Update a field in the processing status."""
    global _processing_status
    _processing_status[key] = value
    # Auto-update timestamp on activity changes
    if key in ("current", "current_book", "last_activity"):
        _processing_status["last_activity_time"] = time.time()


def set_current_book(author: str, title: str, stage: str = "") -> None:
    """Set the currently processing book for status display."""
    global _processing_status
    _processing_status["current_book"] = title or ""
    _processing_status["current_author"] = author or ""
    if stage:
        _processing_status["current"] = stage
    _processing_status["last_activity_time"] = time.time()


def clear_current_book() -> None:
    """Clear the current book (processing finished)."""
    global _processing_status
    _processing_status["current_book"] = ""
    _processing_status["current_author"] = ""
    _processing_status["current_provider"] = ""
    _processing_status["current_step"] = ""
    _processing_status["confidence"] = 0


def set_current_provider(provider: str, step: str = "", is_free: bool = True,
                         chain: list = None, chain_index: int = 0) -> None:
    """Set the current provider/API for status display.

    Args:
        provider: Name of provider (e.g., "Skaldleita", "Gemini", "BookDB API")
        step: Current step (e.g., "Transcribing audio...", "Querying database...")
        is_free: Whether this API is free (not using user's quota)
        chain: Full provider chain if applicable
        chain_index: Current position in chain
    """
    global _processing_status
    _processing_status["current_provider"] = provider
    _processing_status["current_step"] = step
    _processing_status["is_free_api"] = is_free
    if chain:
        _processing_status["provider_chain"] = chain
        _processing_status["provider_index"] = chain_index
    _processing_status["last_activity_time"] = time.time()


def set_api_latency(latency_ms: int) -> None:
    """Record API call latency."""
    global _processing_status
    _processing_status["api_latency_ms"] = latency_ms


def set_confidence(confidence: int) -> None:
    """Set current identification confidence (0-100)."""
    global _processing_status
    _processing_status["confidence"] = confidence


def process_all_queue(
    config: Dict,
    get_db: Callable,
    load_config: Callable,
    is_circuit_open: Callable,
    get_circuit_breaker: Callable,
    check_rate_limit: Callable,
    process_layer_1_audio: Callable,
    process_layer_3_audio: Callable,
    process_layer_1_api: Callable,
    process_queue: Callable,
    process_sl_requeue_verification: Optional[Callable] = None
) -> Tuple[int, int]:
    """Process ALL items in the queue using AUDIO-FIRST identification.

    NEW layered processing (audio is source of truth):
    - Layer 1: Audio transcription + AI parsing (narrator announces the book)
    - Layer 2: AI audio clip analysis (if transcription unclear)
    - SL Requeue Check: Re-verify books after nightly merge (Phase 5)
    - Layer 3: API enrichment (add series, year, etc. - NOT identification)
    - Layer 4: Folder name fallback (last resort, low confidence)

    Philosophy: The audio content IS the book. Folder names can be wrong.

    Args:
        config: Configuration dict
        get_db: Function to get database connection
        load_config: Function to reload config
        is_circuit_open: Function to check circuit breaker status
        get_circuit_breaker: Function to get circuit breaker state
        check_rate_limit: Function to check rate limits
        process_layer_1_audio: Layer 1 audio processing function
        process_layer_3_audio: Layer 2/3 audio processing function
        process_layer_1_api: Layer 3 API processing function
        process_queue: Layer 4 AI queue processing function
        process_sl_requeue_verification: SL requeue verification function (Phase 5)

    Returns:
        Tuple of (total_processed, total_fixed)
    """
    global _processing_status

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as count FROM queue')
    total = c.fetchone()['count']
    conn.close()

    if total == 0:
        logger.info("Queue is empty, nothing to process")
        return 0, 0  # (total_processed, total_fixed)

    # Calculate delay between batches based on rate limit
    user_max = config.get('max_requests_per_hour', 30)
    max_per_hour = max(10, min(user_max, 500))
    min_delay = max(2, 3600 // max_per_hour)
    logger.info(f"Rate limit: {max_per_hour}/hour, delay between batches: {min_delay}s")

    _processing_status = {
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
        "last_activity_time": time.time()
    }
    logger.info(f"=== STARTING AUDIO-FIRST PROCESSING: {total} items in queue ===")

    # Issue #62: Clean up stuck queue items (needs_attention or verified items shouldn't be in queue)
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

    # Advance any items stuck at Layer 2 from previous runs (Layer 2 disabled by default)
    if not config.get('enable_audio_analysis', False):
        conn = get_db()
        c = conn.cursor()
        # Get book IDs first so we can ensure they're in the queue
        c.execute('SELECT id FROM books WHERE verification_layer = 2 AND status = "pending"')
        stuck_books = [row['id'] for row in c.fetchall()]
        if stuck_books:
            c.execute('UPDATE books SET verification_layer = 4 WHERE verification_layer = 2 AND status = "pending"')
            # Ensure all are in the queue for processing
            for book_id in stuck_books:
                c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                if not c.fetchone():
                    c.execute('INSERT INTO queue (book_id, reason, priority) VALUES (?, ?, ?)',
                             (book_id, 'startup_layer2_recovery', 5))
            conn.commit()
            logger.info(f"Advanced {len(stuck_books)} stuck items from Layer 2 to Layer 4")
        conn.close()

    total_processed = 0
    total_fixed = 0

    # NEW LAYER 1: Audio Transcription (narrator announces the book)
    # This is now the PRIMARY identification method
    if config.get('enable_audio_identification', True):  # New setting, defaults to True
        logger.info("=== LAYER 1: Audio Transcription + AI Parsing ===")
        _processing_status["layer"] = 1
        _processing_status["layer_name"] = LAYER_NAMES[1]
        _processing_status["current"] = "Transcribing audio intro via Skaldleita..."
        _processing_status["last_activity"] = "Started audio identification"
        _processing_status["last_activity_time"] = time.time()
        layer1_processed = 0
        layer1_resolved = 0
        while True:
            # Issue #74: Check if Skaldleita circuit breaker is open - wait instead of skipping
            if is_circuit_open('bookdb'):
                cb = get_circuit_breaker('bookdb')
                remaining = int(cb.get('circuit_open_until', 0) - time.time())
                if remaining > 0:
                    wait_time = min(remaining, 60)
                    logger.info(f"[LAYER 1] Skaldleita circuit breaker open, waiting {wait_time}s ({remaining}s total remaining)")
                    _processing_status["current"] = f"Layer 1: Waiting for Skaldleita ({remaining}s)"
                    time.sleep(wait_time)
                    continue

            processed, resolved = process_layer_1_audio(config)
            if processed == 0:
                break
            layer1_processed += processed
            layer1_resolved += resolved
            _processing_status["processed"] = layer1_processed
            time.sleep(2)  # Audio processing needs more time
        logger.info(f"Layer 1 complete: {layer1_processed} items processed, {layer1_resolved} resolved via audio")
        total_processed += layer1_processed
        total_fixed += layer1_resolved

    # LAYER 2: AI Audio Clip Analysis (if transcription was unclear)
    # Sends a longer audio sample to AI for deeper analysis
    if config.get('enable_audio_analysis', False):
        logger.info("=== LAYER 2: AI Audio Clip Analysis (for unclear L1 results) ===")
        _processing_status["layer"] = 2
        _processing_status["layer_name"] = LAYER_NAMES[2]
        _processing_status["current"] = "Sending audio clip to Gemini AI..."
        _processing_status["last_activity"] = "Started AI audio analysis"
        _processing_status["last_activity_time"] = time.time()
        layer2_processed = 0
        layer2_resolved = 0
        circuit_wait_count = 0
        while True:
            # Check if Gemini circuit breaker is open - wait instead of skipping
            if is_circuit_open('gemini'):
                cb = get_circuit_breaker('gemini')
                remaining = int(cb.get('circuit_open_until', 0) - time.time())
                if remaining > 0:
                    circuit_wait_count += 1
                    wait_time = min(remaining, 60)  # Wait max 60s at a time
                    logger.info(f"[LAYER 2] Gemini circuit breaker open, waiting {wait_time}s ({remaining}s total remaining)")
                    _processing_status["current"] = f"Layer 2: Waiting for Gemini ({remaining}s)"
                    time.sleep(wait_time)
                    # Issue #74: Keep waiting for circuit breaker to close - don't skip layers
                    # The circuit breaker will close after its cooldown period (now 5 min)
                    continue

            processed, resolved = process_layer_3_audio(config, verification_layer=2)  # Process Layer 2 items
            if processed == 0:
                break
            circuit_wait_count = 0  # Reset wait count on successful processing
            layer2_processed += processed
            layer2_resolved += resolved
            total_processed += processed
            total_fixed += resolved
            _processing_status["processed"] = total_processed
            time.sleep(2)  # Audio processing needs more time
        logger.info(f"Layer 2 complete: {layer2_processed} items processed, {layer2_resolved} resolved via AI audio")
    else:
        # Layer 2 is disabled - advance any items stuck at verification_layer=2 to Layer 4
        # This ensures they get processed by the folder fallback instead of being orphaned
        conn = get_db()
        c = conn.cursor()
        # Get the book IDs first so we can add them to the queue
        c.execute('SELECT id FROM books WHERE verification_layer = 2 AND status = "pending"')
        layer2_books = [row['id'] for row in c.fetchall()]
        if layer2_books:
            # Update verification_layer
            c.execute('UPDATE books SET verification_layer = 4 WHERE verification_layer = 2 AND status = "pending"')
            # Ensure all advanced items are in the queue
            for book_id in layer2_books:
                c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                if not c.fetchone():
                    c.execute('INSERT INTO queue (book_id, reason, priority) VALUES (?, ?, ?)',
                             (book_id, 'layer2_fallback', 5))
            conn.commit()
            logger.info(f"Layer 2 disabled - advanced {len(layer2_books)} items to Layer 4 (folder fallback)")
        conn.close()

    # SL REQUEUE CHECK (Phase 5): Re-verify books after nightly merge
    # Books with sl_requeue set had partial ID from SL - check if now in main DB
    if process_sl_requeue_verification:
        logger.info("=== SL REQUEUE CHECK: Re-verifying pending books after nightly merge ===")
        _processing_status["current"] = "Checking SL requeue verifications..."
        _processing_status["last_activity"] = "Re-verifying books after nightly merge"
        _processing_status["last_activity_time"] = time.time()

        requeue_processed, requeue_upgraded = process_sl_requeue_verification(config)
        if requeue_processed > 0:
            logger.info(f"SL Requeue Check complete: {requeue_processed} processed, {requeue_upgraded} upgraded")

    # LAYER 3: API Enrichment (NOT identification - add series, year, description, etc.)
    # At this point we should know the book - now we enrich it with metadata
    if config.get('enable_api_lookups', True):
        logger.info("=== LAYER 3: API Enrichment (adding metadata to identified books) ===")
        _processing_status["layer"] = 3
        _processing_status["layer_name"] = LAYER_NAMES[3]
        _processing_status["current"] = "Looking up metadata from Skaldleita/Audnexus..."
        _processing_status["last_activity"] = "Started API metadata lookup"
        _processing_status["last_activity_time"] = time.time()
        layer3_processed = 0
        while True:
            processed, resolved = process_layer_1_api(config)  # Existing API lookup
            if processed == 0:
                break
            layer3_processed += processed
            total_processed += processed
            _processing_status["processed"] = total_processed
            time.sleep(0.5)  # APIs are fast
        logger.info(f"Layer 3 complete: {layer3_processed} items enriched via API")

    # LAYER 4: Folder Name Fallback (LAST RESORT - low confidence)
    # Only used when audio-based identification completely failed
    # Folder names CAN be wrong - this is why confidence is set LOW
    logger.info("=== LAYER 4: Folder Name Fallback (last resort, low confidence) ===")
    _processing_status["layer"] = 4
    _processing_status["layer_name"] = LAYER_NAMES[4]
    _processing_status["current"] = "Verifying identification with AI..."
    _processing_status["last_activity"] = "Started AI verification of folder names"
    _processing_status["last_activity_time"] = time.time()

    batch_num = 0
    rate_limit_hits = 0
    empty_batch_count = 0
    layer4_processed = 0
    layer4_fixed = 0

    while True:
        config = load_config()

        allowed, calls_made, max_calls = check_rate_limit(config)
        if not allowed:
            rate_limit_hits += 1
            wait_time = min(300 * rate_limit_hits, 1800)
            logger.info(f"Rate limit reached ({calls_made}/{max_calls}), waiting {wait_time//60} minutes...")
            _processing_status["current"] = f"Rate limited, waiting {wait_time//60}min..."
            time.sleep(wait_time)
            continue

        batch_num += 1
        logger.info(f"--- Layer 4 batch {batch_num} (API: {calls_made}/{max_calls}) ---")

        # process_queue uses AI to verify folder-based guesses
        # At this point, we're trusting folder names as a last resort
        processed, fixed = process_queue(config, verification_layer=4)

        if processed == 0:
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
                    logger.info(f"Layer 4 cannot process remaining {remaining} items")
                    break
                time.sleep(10)
                continue

        empty_batch_count = 0
        layer4_processed += processed
        layer4_fixed += fixed
        total_processed += processed
        total_fixed += fixed
        _processing_status["processed"] = total_processed
        logger.info(f"Layer 4 Batch {batch_num}: {processed} processed, {fixed} fixed")
        time.sleep(min_delay)

    logger.info(f"Layer 4 complete: {layer4_processed} items processed, {layer4_fixed} fixed via folder fallback")

    # Reset status to idle
    _processing_status["active"] = False
    _processing_status["layer"] = 0
    _processing_status["layer_name"] = "Idle"
    _processing_status["current"] = "Processing complete"
    _processing_status["current_book"] = ""
    _processing_status["current_author"] = ""
    _processing_status["queue_remaining"] = 0
    _processing_status["last_activity"] = f"Completed: {total_processed} processed, {total_fixed} fixed"
    _processing_status["last_activity_time"] = time.time()
    logger.info(f"=== LAYERED PROCESSING COMPLETE: {total_processed} processed, {total_fixed} fixed ===")
    return total_processed, total_fixed


def background_worker(
    load_config: Callable,
    scan_library: Callable,
    process_all_queue_func: Callable
):
    """Background worker that periodically scans and processes.

    Args:
        load_config: Function to load configuration
        scan_library: Function to scan library
        process_all_queue_func: Function to process queue (should be a wrapper that passes dependencies)
    """
    global _worker_running

    logger.info("Background worker thread started")

    while _worker_running:
        config = load_config()

        if config.get('enabled', True):
            try:
                logger.debug("Worker: Starting scan cycle")
                # Scan library
                scan_library(config)

                # Process queue (auto_fix setting controls whether fixes are applied or sent to pending)
                logger.debug("Worker: Processing queue")
                process_all_queue_func(config)
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)

        # Sleep for scan interval
        interval = config.get('scan_interval_hours', 6) * 3600
        logger.debug(f"Worker: Sleeping for {interval} seconds")
        for _ in range(int(interval / 10)):
            if not _worker_running:
                break
            time.sleep(10)

    logger.info("Background worker thread stopped")


def watch_folder_worker(
    load_config: Callable,
    process_watch_folder: Callable
):
    """Background worker that monitors watch folder for new audiobooks.

    Args:
        load_config: Function to load configuration
        process_watch_folder: Function to process watch folder
    """
    global _watch_worker_running

    logger.info("Watch folder worker thread started")

    while _watch_worker_running:
        config = load_config()

        if config.get('watch_mode', False) and config.get('watch_folder', '').strip():
            try:
                process_watch_folder(config)
            except Exception as e:
                logger.error(f"Watch folder worker error: {e}", exc_info=True)

        # Sleep for watch interval (default 60 seconds)
        interval = config.get('watch_interval_seconds', 60)
        for _ in range(int(interval)):
            if not _watch_worker_running:
                break
            time.sleep(1)

    logger.info("Watch folder worker thread stopped")


def start_worker(
    load_config: Callable,
    scan_library: Callable,
    process_all_queue_func: Callable,
    process_watch_folder: Callable
):
    """Start the background worker.

    Args:
        load_config: Function to load configuration
        scan_library: Function to scan library
        process_all_queue_func: Function to process queue
        process_watch_folder: Function to process watch folder
    """
    global _worker_thread, _worker_running, _watch_worker_thread, _watch_worker_running

    if _worker_thread and _worker_thread.is_alive():
        logger.info("Worker already running")
    else:
        _worker_running = True
        _worker_thread = threading.Thread(
            target=background_worker,
            args=(load_config, scan_library, process_all_queue_func),
            daemon=True
        )
        _worker_thread.start()
        logger.info("Background worker started")

    # Also start watch folder worker
    config = load_config()
    if config.get('watch_mode', False) and config.get('watch_folder', '').strip():
        if not (_watch_worker_thread and _watch_worker_thread.is_alive()):
            _watch_worker_running = True
            _watch_worker_thread = threading.Thread(
                target=watch_folder_worker,
                args=(load_config, process_watch_folder),
                daemon=True
            )
            _watch_worker_thread.start()
            logger.info("Watch folder worker started")


def stop_worker():
    """Stop the background worker."""
    global _worker_running, _watch_worker_running
    _worker_running = False
    _watch_worker_running = False
    logger.info("Background worker stop requested")


def is_worker_running() -> bool:
    """Check if worker is actually running."""
    global _worker_thread, _worker_running
    return _worker_running and _worker_thread is not None and _worker_thread.is_alive()


__all__ = [
    'process_all_queue',
    'background_worker',
    'watch_folder_worker',
    'start_worker',
    'stop_worker',
    'is_worker_running',
    'get_processing_status',
    'update_processing_status',
    'set_current_book',
    'clear_current_book',
    'LAYER_NAMES',
]
