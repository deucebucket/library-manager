#!/bin/bash
# Library Manager Integration Test Suite
# Tests Docker deployment and core functionality
#
# Usage: ./run-integration-tests.sh [--rebuild] [--local]
#   --rebuild: Regenerate test library before testing
#   --local:   Build from local source instead of pulling ghcr.io image

# Don't exit on error - we want to run all tests
# set -e

TEST_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_PORT=5858
CONTAINER_NAME="library-manager-test"
PASSED=0
FAILED=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; ((PASSED++)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((FAILED++)); }
log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# ==========================================
# SETUP
# ==========================================
setup() {
    log_info "Setting up test environment..."

    # Generate test library if needed
    if [[ "$1" == "--rebuild" ]] || [[ ! -d "$TEST_DIR/test-audiobooks" ]]; then
        log_info "Generating 2GB test audiobook library..."
        "$TEST_DIR/generate-test-library.sh" "$TEST_DIR/test-audiobooks"
    fi

    # Create fresh data directory
    rm -rf "$TEST_DIR/fresh-deploy/data"
    mkdir -p "$TEST_DIR/fresh-deploy/data"

    # Stop existing test container
    podman stop "$CONTAINER_NAME" 2>/dev/null || true
    podman rm "$CONTAINER_NAME" 2>/dev/null || true

    # Build from local source or pull image
    if [[ "$1" == "--local" ]] || [[ "$2" == "--local" ]]; then
        log_info "Building from local source..."
        podman build -t library-manager:local-test "$TEST_DIR/.." >/dev/null 2>&1
        IMAGE="library-manager:local-test"
    else
        log_info "Pulling latest image from ghcr.io..."
        podman pull ghcr.io/deucebucket/library-manager:latest
        IMAGE="ghcr.io/deucebucket/library-manager:latest"
    fi

    # Create config with library path
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

    # Start container
    # Use slirp4netns networking (default) - container cannot access host localhost
    # This simulates a real user environment without access to local BookDB
    log_info "Starting Library Manager container (isolated network)..."
    podman run -d --name "$CONTAINER_NAME" \
        -p "$TEST_PORT:5757" \
        -v "$TEST_DIR/test-audiobooks:/audiobooks:rw" \
        -v "$TEST_DIR/fresh-deploy/data:/data" \
        --add-host=localhost:127.0.0.1 \
        "$IMAGE"

    # Wait for startup
    log_info "Waiting for container to start..."
    sleep 5

    # Wait for scan to complete
    for i in {1..30}; do
        if curl -s "http://localhost:$TEST_PORT/api/stats" | grep -q '"total_books"'; then
            break
        fi
        sleep 1
    done
}

# ==========================================
# TESTS
# ==========================================

test_container_running() {
    log_info "Test: Container is running"
    if podman ps | grep -q "$CONTAINER_NAME"; then
        log_pass "Container is running"
    else
        log_fail "Container is not running"
        podman logs "$CONTAINER_NAME" 2>&1 | tail -20
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
    response=$(curl -s "http://localhost:$TEST_PORT/api/queue")

    # Check for reversed structure detection (Metro 2033)
    if echo "$response" | grep -q "Metro 2033"; then
        log_pass "Detected reversed structure (Metro 2033)"
    else
        log_fail "Did not detect reversed structure"
    fi

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

# ==========================================
# CLEANUP
# ==========================================
cleanup() {
    log_info "Cleaning up..."
    podman stop "$CONTAINER_NAME" 2>/dev/null || true
    podman rm "$CONTAINER_NAME" 2>/dev/null || true
}

# ==========================================
# MAIN
# ==========================================
main() {
    echo "=========================================="
    echo "Library Manager Integration Tests"
    echo "=========================================="
    echo ""

    # Setup
    setup "$1"

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
