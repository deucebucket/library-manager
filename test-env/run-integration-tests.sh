#!/bin/bash
# Library Manager Integration Test Suite
# Tests Docker deployment and core functionality
#
# Usage: ./run-integration-tests.sh [--rebuild] [--local] [--offline] [--runtime docker|podman|auto]
#   --rebuild: Regenerate test library before testing
#   --local:   Build from local source instead of pulling ghcr.io image
#   --offline: Disable network-dependent pipeline layers for no-network environments
#   --runtime: Override container runtime (default: auto)

# Don't exit on error - we want to run all tests
# set -e

TEST_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_PORT=5858
CONTAINER_NAME="library-manager-test"
PASSED=0
FAILED=0
REBUILD=0
LOCAL_IMAGE=0
OFFLINE_MODE=0
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-auto}"
RUNTIME_CMD=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; ((PASSED++)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((FAILED++)); }
log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

resolve_runtime() {
    if [[ "$CONTAINER_RUNTIME" == "auto" || -z "$CONTAINER_RUNTIME" ]]; then
        if command -v podman >/dev/null 2>&1; then
            RUNTIME_CMD="podman"
            return
        fi
        if command -v docker >/dev/null 2>&1; then
            RUNTIME_CMD="docker"
            return
        fi
        log_fail "No container runtime found. Install podman or docker."
        exit 1
    fi

    if [[ "$CONTAINER_RUNTIME" != "podman" && "$CONTAINER_RUNTIME" != "docker" ]]; then
        log_fail "Unsupported runtime '$CONTAINER_RUNTIME'. Use podman, docker, or auto."
        exit 1
    fi

    if ! command -v "$CONTAINER_RUNTIME" >/dev/null 2>&1; then
        log_fail "Runtime '$CONTAINER_RUNTIME' not found."
        exit 1
    fi
    RUNTIME_CMD="$CONTAINER_RUNTIME"
}

container_exec() {
    "$RUNTIME_CMD" "$@"
}

# ==========================================
# SETUP
# ==========================================
setup() {
    log_info "Setting up test environment..."

    # Generate test library if needed
    if [[ "$REBUILD" -eq 1 ]] || [[ ! -d "$TEST_DIR/test-audiobooks" ]]; then
        log_info "Generating 2GB test audiobook library..."
        "$TEST_DIR/generate-test-library.sh" "$TEST_DIR/test-audiobooks"
    fi

    # Create fresh data directory
    rm -rf "$TEST_DIR/fresh-deploy/data"
    mkdir -p "$TEST_DIR/fresh-deploy/data"

    # Stop existing test container
    container_exec stop "$CONTAINER_NAME" 2>/dev/null || true
    container_exec rm "$CONTAINER_NAME" 2>/dev/null || true

    # Build from local source or pull image
    if [[ "$LOCAL_IMAGE" -eq 1 ]]; then
        log_info "Building from local source..."
        container_exec build -t library-manager:local-test "$TEST_DIR/.." >/dev/null 2>&1
        IMAGE="library-manager:local-test"
    else
        log_info "Pulling latest image from ghcr.io..."
        container_exec pull ghcr.io/deucebucket/library-manager:latest
        IMAGE="ghcr.io/deucebucket/library-manager:latest"
    fi

    # Create config with library path
    if [[ "$OFFLINE_MODE" -eq 1 ]]; then
        log_info "Running in offline mode - disabling network-dependent pipeline layers"
        cat > "$TEST_DIR/fresh-deploy/data/config.json" << 'EOF'
{
  "library_paths": ["/audiobooks"],
  "ai_provider": "gemini",
  "openrouter_model": "",
  "scan_interval_hours": 6,
  "auto_fix": false,
  "enabled": true,
  "enable_audio_identification": false,
  "enable_api_lookups": false,
  "enable_ai_verification": false,
  "enable_audio_analysis": false,
  "enable_content_analysis": false,
  "strict_language_matching": true,
  "detect_language_from_audio": false
}
EOF
        if [[ -f "$TEST_DIR/../secrets.json" ]]; then
            cp "$TEST_DIR/../secrets.json" "$TEST_DIR/fresh-deploy/data/secrets.json"
            log_info "Copied secrets.json for consistency (offline mode should not depend on it)"
        else
            log_info "No secrets.json found - offline mode will not use external providers"
        fi
    else
        cat > "$TEST_DIR/fresh-deploy/data/config.json" << 'EOF'
{
  "library_paths": ["/audiobooks"],
  "ai_provider": "openrouter",
  "openrouter_model": "google/gemma-3n-e4b-it:free",
  "scan_interval_hours": 6,
  "auto_fix": false,
  "enabled": true
}
EOF

        # Copy secrets from project root if available (needed for AI processing tests)
        if [[ -f "$TEST_DIR/../secrets.json" ]]; then
            cp "$TEST_DIR/../secrets.json" "$TEST_DIR/fresh-deploy/data/secrets.json"
            log_info "Copied API secrets for integration testing"
        else
            log_info "WARNING: No secrets.json found - AI processing tests will fail"
        fi
    fi

    # Start container
    # Use slirp4netns networking (default) - container cannot access host localhost
    # This simulates a real user environment without access to local BookDB
    log_info "Starting Library Manager container (isolated network)..."
    container_exec run -d --name "$CONTAINER_NAME" \
        -p "$TEST_PORT:5757" \
        -v "$TEST_DIR/test-audiobooks:/audiobooks:rw" \
        -v "$TEST_DIR/fresh-deploy/data:/data" \
        --add-host=localhost:127.0.0.1 \
        "$IMAGE"

    # Wait for startup
    log_info "Waiting for container to start..."
    sleep 5

    # Wait for API to be ready
    for i in {1..30}; do
        if curl -s "http://localhost:$TEST_PORT/api/stats" | grep -q '"total_books"'; then
            break
        fi
        sleep 1
    done

    # Wait for scan to actually complete (queue should have items)
    log_info "Waiting for scan to complete..."
    for i in {1..60}; do
        if [[ "$OFFLINE_MODE" -eq 1 ]]; then
            book_count=$(curl -s "http://localhost:$TEST_PORT/api/stats" | python3 -c "import json,sys; print(json.load(sys.stdin).get('total_books', 0))" 2>/dev/null || echo "0")
            if [[ "$book_count" -gt 3 ]]; then
                log_info "Offline scan indexed $book_count books"
                break
            fi
        else
            queue_count=$(curl -s "http://localhost:$TEST_PORT/api/queue" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['count'])" 2>/dev/null || echo "0")
            if [[ "$queue_count" -gt 3 ]]; then
                log_info "Scan populated queue with $queue_count items"
                break
            fi
        fi
        sleep 1
    done
}

# ==========================================
# TESTS
# ==========================================

test_container_running() {
    log_info "Test: Container is running"
    if container_exec ps | grep -q "$CONTAINER_NAME"; then
        log_pass "Container is running"
    else
        log_fail "Container is not running"
        container_exec logs "$CONTAINER_NAME" 2>&1 | tail -20
        return 1
    fi
}

test_web_ui_accessible() {
    log_info "Test: Web UI accessible"
    status=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$TEST_PORT/")
    if [[ "$status" == "200" ]]; then
        log_pass "Web UI returns 200 OK"
    else
        log_fail "Web UI returned $status"
    fi
}

test_stats_endpoint() {
    log_info "Test: Stats API endpoint"
    response=$(curl -s "http://localhost:$TEST_PORT/api/stats")

    if echo "$response" | grep -q '"total_books"'; then
        total=$(echo "$response" | python3 -c "import json,sys; print(json.load(sys.stdin)['total_books'])")
        log_pass "Stats endpoint works - found $total books"
    else
        log_fail "Stats endpoint failed"
        echo "Response: $response"
    fi
}

test_queue_endpoint() {
    log_info "Test: Queue API endpoint"
    response=$(curl -s "http://localhost:$TEST_PORT/api/queue")

    if echo "$response" | grep -q '"items"'; then
        count=$(echo "$response" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['items']))")
        log_pass "Queue endpoint works - $count items in queue"
    else
        log_fail "Queue endpoint failed"
    fi
}

test_scan_detected_issues() {
    log_info "Test: Scanner detected expected issues"
    if [[ "$OFFLINE_MODE" -eq 1 ]]; then
        total=$(curl -s "http://localhost:$TEST_PORT/api/stats" | python3 -c "import json,sys; print(json.load(sys.stdin).get('total_books', 0))" 2>/dev/null || echo "0")
        if [[ "$total" -gt 0 ]]; then
            log_pass "Offline scan indexed $total books; network-driven issue routing is out of scope"
        else
            log_fail "Offline scan produced no durable inventory"
        fi
        return
    fi
    response=$(curl -s "http://localhost:$TEST_PORT/api/queue")

    # NOTE: Reversed structure detection was removed in beta.69 (Issue #52 - false positives)
    # Items like Metro 2033/Dmitry Glukhovsky now go through normal API lookup flow
    # and end up in "needs_attention" if APIs can't identify them.

    # Check for missing author detection (The Expanse)
    if echo "$response" | grep -q "The Expanse"; then
        log_pass "Detected missing author (The Expanse)"
    else
        log_fail "Did not detect missing author"
    fi
}

test_history_endpoint() {
    log_info "Test: History API endpoint"
    response=$(curl -s "http://localhost:$TEST_PORT/api/recent_history")

    if echo "$response" | grep -q '"items"'; then
        log_pass "History endpoint works"
    else
        log_fail "History endpoint failed: $response"
    fi
}

test_scan_trigger() {
    log_info "Test: Manual scan trigger"
    response=$(curl -s -X POST "http://localhost:$TEST_PORT/api/scan")

    if echo "$response" | grep -q '"success"'; then
        log_pass "Scan trigger works"
    else
        log_fail "Scan trigger failed"
    fi
}

test_no_local_db_dependency() {
    log_info "Test: Works without local BookDB"
    # The container doesn't have access to /mnt/rag_data/bookdb
    # It should still function using pattern-based detection

    stats=$(curl -s "http://localhost:$TEST_PORT/api/stats")
    total=$(echo "$stats" | python3 -c "import json,sys; print(json.load(sys.stdin)['total_books'])")

    if [[ "$total" -gt 0 ]]; then
        log_pass "Functions without local BookDB ($total books detected)"
    else
        log_fail "No books detected - may require local DB"
    fi
}

test_process_empties_queue() {
    log_info "Test: Processing actually processes queue items"
    # CRITICAL TEST: This catches the beta.45 bug where process returned 0 but queue stayed full
    # NOTE: Processing is rate-limited (~30s per batch of 3), so we need realistic wait times

    # Get initial queue count
    initial=$(curl -s "http://localhost:$TEST_PORT/api/queue" | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))")

    if [[ "$initial" -eq 0 ]]; then
        log_pass "Queue already empty (nothing to process)"
        return
    fi

    log_info "Queue has $initial items, triggering process..."

    # Trigger processing (with timeout)
    curl -s -X POST "http://localhost:$TEST_PORT/api/process" \
        -H "Content-Type: application/json" \
        -d '{"all": true}' \
        --max-time 180 >/dev/null 2>&1 &

    # Wait for processing with progress checks (realistic timing for rate-limited APIs)
    # Each batch of 3 items takes ~30-60s due to API rate limits, so wait up to 150s
    for i in {1..15}; do
        sleep 10
        status=$(curl -s "http://localhost:$TEST_PORT/api/process_status")
        processed=$(echo "$status" | python3 -c "import json,sys; print(json.load(sys.stdin).get('processed', 0))" 2>/dev/null || echo "0")
        if [[ "$processed" -gt 0 ]]; then
            log_pass "Processing working: $processed items processed after $((i*10))s"
            return
        fi
        # Also check if queue reduced
        current=$(curl -s "http://localhost:$TEST_PORT/api/queue" | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))" 2>/dev/null || echo "$initial")
        if [[ "$current" -lt "$initial" ]]; then
            log_pass "Queue reduced from $initial to $current after $((i*10))s"
            return
        fi
        log_info "  ...waiting ($((i*10))s elapsed, queue: $current)"
    done

    # Final check after 150s
    final=$(curl -s "http://localhost:$TEST_PORT/api/queue" | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))")
    if [[ "$final" -lt "$initial" ]]; then
        log_pass "Queue reduced from $initial to $final"
    else
        if [[ "$OFFLINE_MODE" -eq 1 ]]; then
            log_pass "Offline mode: processing layers disabled; queue remained at $final"
        else
            log_fail "CRITICAL: Process returned 0 and queue unchanged ($initial items) after 150s - processing bug!"
        fi
    fi
}

test_queue_items_not_stuck() {
    log_info "Test: Queue items are not stuck at invalid verification layers"
    # Items should not be stuck at layer 4 in the queue (that's the bug this test catches)

    # The production image intentionally contains Python rather than the
    # sqlite3 shell. Use the runtime already required by the application, and
    # never turn an exec/import/query failure into the truthful number zero.
    if ! stuck=$(container_exec exec "$CONTAINER_NAME" python3 -c \
        "import sqlite3; c=sqlite3.connect('/data/library.db'); print(c.execute(\"SELECT COUNT(*) FROM queue q JOIN books b ON q.book_id = b.id WHERE b.verification_layer = 4\").fetchone()[0])" 2>/dev/null); then
        log_fail "Could not query the container database for invalid queue layers"
        return
    fi
    if [[ ! "$stuck" =~ ^[0-9]+$ ]]; then
        log_fail "Container database query returned a non-numeric result"
        return
    fi

    if [[ "$stuck" -eq 0 ]]; then
        log_pass "No items stuck at layer 4"
    else
        log_fail "CRITICAL: $stuck items stuck at layer 4 in queue - processing bug!"
    fi
}

test_book_verification() {
    log_info "Test: Book identification verification"
    # Runs the Python verification test that checks:
    # - Real books are correctly identified
    # - Problem patterns are detected (reversed structure, missing author)
    # - Series folders are detected
    # - Queue reasons are correct

    if ! command -v python3 &> /dev/null; then
        log_info "python3 not available, skipping verification test"
        return
    fi

    if [[ "$OFFLINE_MODE" -eq 1 ]]; then
        # Offline mode deliberately disables the lookup and AI layers that
        # produce the fixture's expected identities. Its honest contract is a
        # readable, non-empty persisted scan, not online verification labels.
        if result=$(python3 -c \
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); ok=c.execute('PRAGMA quick_check').fetchone()[0]; count=c.execute('SELECT COUNT(*) FROM books').fetchone()[0]; print(f'{ok}:{count}')" \
            "$TEST_DIR/fresh-deploy/data/library.db" 2>/dev/null) \
                && [[ "$result" =~ ^ok:[1-9][0-9]*$ ]]; then
            log_pass "Offline database is valid with ${result#ok:} indexed books; online identity labels are out of scope"
        else
            log_fail "Offline scan database is absent, corrupt, or empty"
        fi
        return
    fi

    # Run the online verification test against the test database
    if python3 "$TEST_DIR/test-book-verification.py" "$TEST_DIR/fresh-deploy/data/library.db" >/dev/null 2>&1; then
        log_pass "Book identification verification passed"
    else
        log_fail "Book identification verification failed"
        # Run again to show output
        python3 "$TEST_DIR/test-book-verification.py" "$TEST_DIR/fresh-deploy/data/library.db" 2>&1 | tail -20
    fi
}

# ==========================================
# CLEANUP
# ==========================================
cleanup() {
    log_info "Cleaning up..."
    container_exec stop "$CONTAINER_NAME" 2>/dev/null || true
    container_exec rm "$CONTAINER_NAME" 2>/dev/null || true
}

# ==========================================
# MAIN
# ==========================================
main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --rebuild)
                REBUILD=1
                ;;
            --local)
                LOCAL_IMAGE=1
                ;;
            --offline)
                OFFLINE_MODE=1
                ;;
            --runtime)
                CONTAINER_RUNTIME="${2:-auto}"
                shift
                ;;
            *)
                log_info "Unknown argument: $1"
                ;;
        esac
        shift
    done

    resolve_runtime

    echo "=========================================="
    echo "Library Manager Integration Tests"
    echo "=========================================="
    echo ""
    echo "Runtime: $RUNTIME_CMD"
    if [[ "$OFFLINE_MODE" -eq 1 ]]; then
        echo "Mode: offline"
    else
        echo "Mode: online"
    fi
    echo ""

    # Setup
    setup

    echo ""
    echo "=========================================="
    echo "Running Tests"
    echo "=========================================="
    echo ""

    # Run tests
    test_container_running
    test_web_ui_accessible
    test_stats_endpoint
    test_queue_endpoint
    test_scan_detected_issues
    test_history_endpoint
    test_scan_trigger
    test_no_local_db_dependency
    test_process_empties_queue
    test_queue_items_not_stuck
    test_book_verification

    # Cleanup
    echo ""
    cleanup

    # Summary
    echo ""
    echo "=========================================="
    echo "Test Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed: $PASSED${NC}"
    echo -e "${RED}Failed: $FAILED${NC}"
    echo ""

    if [[ $FAILED -eq 0 ]]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed${NC}"
        exit 1
    fi
}

# Handle cleanup on exit
trap cleanup EXIT

main "$@"
