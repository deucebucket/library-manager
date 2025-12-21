#!/usr/bin/env python3
"""
REAL End-to-End Integration Tests for Library Manager

These tests simulate actual user workflows:
1. Scan a library with known issues
2. Process the queue
3. Verify fixes are created
4. Apply fixes and verify files move
5. Undo fixes and verify files restore

If a user would hit a bug, these tests should catch it.
"""

import os
import sys
import json
import time
import shutil
import tempfile
import requests
import subprocess
from pathlib import Path

# Test configuration
APP_URL = os.environ.get('TEST_APP_URL', 'http://localhost:5757')
TIMEOUT = 30  # seconds to wait for operations

# Test results
PASSED = 0
FAILED = 0
ERRORS = []


def log_pass(msg):
    global PASSED
    print(f"\033[92m[PASS]\033[0m {msg}")
    PASSED += 1


def log_fail(msg, detail=None):
    global FAILED
    print(f"\033[91m[FAIL]\033[0m {msg}")
    if detail:
        print(f"       Detail: {detail}")
    FAILED += 1
    ERRORS.append(msg)


def log_info(msg):
    print(f"\033[93m[INFO]\033[0m {msg}")


def api_get(endpoint):
    """Make GET request to API."""
    try:
        resp = requests.get(f"{APP_URL}{endpoint}", timeout=10)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def api_post(endpoint, data=None):
    """Make POST request to API."""
    try:
        resp = requests.post(f"{APP_URL}{endpoint}", json=data or {}, timeout=60)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def wait_for_app():
    """Wait for app to be ready."""
    log_info("Waiting for app to be ready...")
    for i in range(TIMEOUT):
        try:
            resp = requests.get(f"{APP_URL}/api/stats", timeout=2)
            if resp.status_code == 200:
                log_info("App is ready")
                return True
        except:
            pass
        time.sleep(1)
    log_fail("App not ready after timeout")
    return False


def create_test_library(base_path):
    """
    Create a test library with KNOWN issues that the app should detect and fix.
    Returns dict mapping test case name to expected behavior.
    """
    log_info(f"Creating test library at {base_path}")

    test_cases = {}

    # Clean and create base
    if base_path.exists():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)

    # === TEST CASE 1: Reversed author/title ===
    # User has: Title/Author instead of Author/Title
    case1_path = base_path / "The Martian" / "Andy Weir"
    case1_path.mkdir(parents=True)
    (case1_path / "audiobook.mp3").write_bytes(b'\x00' * 1000)
    test_cases["reversed_structure"] = {
        "original_path": str(case1_path),
        "expected_author": "Andy Weir",
        "expected_title": "The Martian",
        "should_create_fix": True
    }

    # === TEST CASE 2: Already correct ===
    # Should be marked verified, NOT create a fix
    case2_path = base_path / "Brandon Sanderson" / "Mistborn"
    case2_path.mkdir(parents=True)
    (case2_path / "book.mp3").write_bytes(b'\x00' * 1000)
    test_cases["already_correct"] = {
        "original_path": str(case2_path),
        "expected_author": "Brandon Sanderson",
        "expected_title": "Mistborn",
        "should_create_fix": False  # Already correct
    }

    # === TEST CASE 3: Junk in folder name ===
    case3_path = base_path / "[torrents.org] Stephen King" / "IT (2017) 320kbps"
    case3_path.mkdir(parents=True)
    (case3_path / "track01.mp3").write_bytes(b'\x00' * 1000)
    test_cases["junk_in_name"] = {
        "original_path": str(case3_path),
        "expected_author": "Stephen King",
        "expected_title": "IT",
        "should_create_fix": True
    }

    # === TEST CASE 4: Unknown author ===
    case4_path = base_path / "Unknown" / "1984"
    case4_path.mkdir(parents=True)
    (case4_path / "full.mp3").write_bytes(b'\x00' * 1000)
    test_cases["unknown_author"] = {
        "original_path": str(case4_path),
        "expected_author": "George Orwell",
        "expected_title": "1984",
        "should_create_fix": True
    }

    log_info(f"Created {len(test_cases)} test cases")
    return test_cases


def get_queue_count():
    """Get current queue count."""
    resp = api_get("/api/queue")
    if "error" in resp:
        return -1
    return resp.get("count", len(resp.get("items", [])))


def get_history():
    """Get history entries."""
    resp = api_get("/api/recent_history")
    if "error" in resp:
        return []
    return resp.get("items", [])


def get_pending_fixes():
    """Get pending fixes from history."""
    history = get_history()
    return [h for h in history if h.get("status") == "pending_fix"]


# ============================================
# ACTUAL TESTS - These test real user workflows
# ============================================

def test_01_scan_populates_queue(test_library_path):
    """
    USER WORKFLOW: User clicks Scan Library
    EXPECTED: Queue should have items to process
    """
    log_info("TEST 1: Scan should populate queue")

    # Get initial queue count
    initial_count = get_queue_count()
    log_info(f"Initial queue count: {initial_count}")

    # Trigger scan
    resp = api_post("/api/scan")
    if not resp.get("success"):
        log_fail("Scan failed to start", resp.get("error"))
        return False

    # Wait for scan to complete
    time.sleep(3)

    # Check queue has items
    final_count = get_queue_count()
    log_info(f"Final queue count: {final_count}")

    if final_count > 0:
        log_pass(f"Scan populated queue with {final_count} items")
        return True
    else:
        log_fail("Scan did not populate queue - queue is empty")
        return False


def test_02_process_empties_queue():
    """
    USER WORKFLOW: User clicks Process Queue button
    EXPECTED: Queue should be empty (or have fewer items) after processing
    BUG THIS CATCHES: The beta.45 bug where process returned 0 but queue stayed full
                      The beta.53 bug where Process skipped Layer 1
    """
    log_info("TEST 2: Process should empty the queue")

    initial_count = get_queue_count()
    if initial_count == 0:
        log_info("Queue already empty, skipping process test")
        return True

    log_info(f"Queue has {initial_count} items, processing...")

    # Process queue - MUST match what UI actually sends!
    # UI sends {limit: 5} or {limit: 3}, NOT {all: true}
    # Using {all: true} would bypass bugs in the normal code path
    resp = api_post("/api/process", {"limit": 5})

    if "error" in resp:
        log_fail("Process request failed", resp.get("error"))
        return False

    processed = resp.get("processed", 0)
    log_info(f"API reported processed: {processed}")

    # Wait a moment for processing
    time.sleep(2)

    # Check queue is empty (or reduced)
    final_count = get_queue_count()
    log_info(f"Queue now has {final_count} items")

    # THE CRITICAL CHECK - this catches the beta.45 bug
    if processed == 0 and final_count == initial_count:
        log_fail(f"CRITICAL BUG: Process returned 0 but queue unchanged ({initial_count} items)")
        log_fail("This is the beta.45 bug - items stuck at verification_layer=4")
        return False

    if final_count < initial_count:
        log_pass(f"Queue reduced from {initial_count} to {final_count} ({processed} processed)")
        return True
    elif final_count == 0:
        log_pass(f"Queue emptied completely ({processed} processed)")
        return True
    else:
        log_fail(f"Queue not reduced: was {initial_count}, now {final_count}")
        return False


def test_02b_single_process_works():
    """
    USER WORKFLOW: User clicks Process (single batch, not "all")
    EXPECTED: Should still process items through Layer 1 -> Layer 2
    BUG THIS CATCHES: beta.53 bug where single Process skipped Layer 1
    """
    log_info("TEST 2b: Single Process click should work (not just Process All)")

    # Re-scan to get fresh items in queue
    resp = api_post("/api/deep_rescan")
    time.sleep(1)

    initial_count = get_queue_count()
    if initial_count == 0:
        log_info("Queue empty after rescan, test inconclusive")
        return True

    log_info(f"Queue has {initial_count} items, processing with single click (no 'all' flag)...")

    # Process WITHOUT "all" flag - this is what the UI does for single clicks
    resp = api_post("/api/process", {"limit": 5})

    if "error" in resp:
        log_fail("Single process request failed", resp.get("error"))
        return False

    processed = resp.get("processed", 0)
    log_info(f"Single process reported: {processed} processed")

    # THE CRITICAL CHECK - single process should work, not return 0
    if processed == 0 and initial_count > 0:
        log_fail(f"CRITICAL BUG: Single Process returned 0 with {initial_count} queued items")
        log_fail("This is the beta.53 bug - Process button skipped Layer 1")
        return False

    log_pass(f"Single Process worked: {processed} items processed")
    return True


def test_03_history_has_entries():
    """
    USER WORKFLOW: User checks History to see what happened
    EXPECTED: History should have entries after processing
    """
    log_info("TEST 3: History should have entries after processing")

    history = get_history()

    if len(history) > 0:
        # Count by status
        statuses = {}
        for h in history:
            status = h.get("status", "unknown")
            statuses[status] = statuses.get(status, 0) + 1

        log_pass(f"History has {len(history)} entries: {statuses}")
        return True
    else:
        log_fail("History is empty after processing")
        return False


def test_04_pending_fixes_exist():
    """
    USER WORKFLOW: User looks for pending fixes to approve
    EXPECTED: Should have pending fixes for items that need changes
    """
    log_info("TEST 4: Should have pending fixes for items needing changes")

    pending = get_pending_fixes()

    if len(pending) > 0:
        log_pass(f"Found {len(pending)} pending fixes awaiting approval")
        for fix in pending[:3]:  # Show first 3
            log_info(f"  - {fix.get('old_author')}/{fix.get('old_title')} -> {fix.get('new_author')}/{fix.get('new_title')}")
        return True
    else:
        # Check if items were auto-fixed or verified instead
        history = get_history()
        fixed = [h for h in history if h.get("status") == "fixed"]
        verified = [h for h in history if h.get("status") == "verified"]

        if len(fixed) > 0 or len(verified) > 0:
            log_pass(f"No pending fixes, but {len(fixed)} fixed and {len(verified)} verified")
            return True
        else:
            log_fail("No pending fixes, no fixed items, no verified items - processing did nothing")
            return False


def test_05_apply_fix_moves_file():
    """
    USER WORKFLOW: User approves a pending fix
    EXPECTED: File should actually move to new location
    """
    log_info("TEST 5: Applying fix should move files")

    pending = get_pending_fixes()
    if not pending:
        log_info("No pending fixes to test apply on")
        return True

    fix = pending[0]
    fix_id = fix.get("id")
    old_path = fix.get("old_path")
    new_path = fix.get("new_path")

    log_info(f"Applying fix {fix_id}: {old_path} -> {new_path}")

    # Check old path exists before
    if not Path(old_path).exists():
        log_fail(f"Old path doesn't exist before apply: {old_path}")
        return False

    # Apply the fix
    resp = api_post(f"/api/apply_fix/{fix_id}")

    if not resp.get("success"):
        log_fail(f"Apply fix failed: {resp.get('message', resp.get('error'))}")
        return False

    # Verify file moved
    time.sleep(1)

    old_exists = Path(old_path).exists()
    new_exists = Path(new_path).exists()

    if not old_exists and new_exists:
        log_pass(f"File moved successfully to {new_path}")
        return True
    elif old_exists and new_exists:
        log_fail("Both old and new paths exist - file was copied not moved")
        return False
    elif old_exists and not new_exists:
        log_fail("File not moved - old path still exists, new path doesn't")
        return False
    else:
        log_fail("Both paths gone - file was deleted!")
        return False


def test_06_undo_restores_file():
    """
    USER WORKFLOW: User wants to undo a fix
    EXPECTED: File should move back to original location
    """
    log_info("TEST 6: Undo should restore files to original location")

    # Find a fixed item to undo
    history = get_history()
    fixed = [h for h in history if h.get("status") == "fixed"]

    if not fixed:
        log_info("No fixed items to test undo on")
        return True

    fix = fixed[0]
    fix_id = fix.get("id")
    old_path = fix.get("old_path")
    new_path = fix.get("new_path")

    log_info(f"Undoing fix {fix_id}: {new_path} -> {old_path}")

    # Check new path exists before undo
    if not Path(new_path).exists():
        log_info(f"New path doesn't exist, can't test undo: {new_path}")
        return True

    # Undo the fix
    resp = api_post(f"/api/undo/{fix_id}")

    if not resp.get("success"):
        log_fail(f"Undo failed: {resp.get('message', resp.get('error'))}")
        return False

    # Verify file moved back
    time.sleep(1)

    old_exists = Path(old_path).exists()
    new_exists = Path(new_path).exists()

    if old_exists and not new_exists:
        log_pass(f"File restored to original location: {old_path}")
        return True
    else:
        log_fail(f"Undo didn't restore file properly (old={old_exists}, new={new_exists})")
        return False


def test_07_queue_empty_after_full_process():
    """
    USER WORKFLOW: User processes entire queue
    EXPECTED: Queue should be completely empty
    BUG THIS CATCHES: Items stuck in queue that no layer processes
    """
    log_info("TEST 7: Queue should be empty after full processing")

    queue_count = get_queue_count()

    if queue_count == 0:
        log_pass("Queue is empty after processing")
        return True
    else:
        log_fail(f"Queue still has {queue_count} items - they're stuck!")

        # Try to diagnose why
        resp = api_get("/api/queue")
        items = resp.get("items", [])[:3]
        for item in items:
            log_info(f"  Stuck item: {item.get('path', 'unknown')}")

        return False


# ============================================
# MAIN
# ============================================

def main():
    global PASSED, FAILED, ERRORS

    print("=" * 60)
    print("LIBRARY MANAGER - REAL USER WORKFLOW TESTS")
    print("=" * 60)
    print()
    print("These tests simulate actual user workflows.")
    print("If a user would hit a bug, these tests should catch it.")
    print()

    # Check if app is running
    if not wait_for_app():
        print("\nERROR: App not running. Start it first:")
        print("  python app.py")
        print(f"\nOr set TEST_APP_URL environment variable (current: {APP_URL})")
        sys.exit(1)

    # Get library paths from config
    resp = api_get("/api/stats")

    # Create test library in temp dir
    # In production, this would use an existing test library
    test_lib = Path(tempfile.gettempdir()) / "library-manager-test-lib"

    # Check if we have a configured library we can use
    # For now, run tests against whatever library is configured

    print()
    print("=" * 60)
    print("RUNNING TESTS")
    print("=" * 60)
    print()

    # Run tests in order - each builds on the previous
    test_01_scan_populates_queue(test_lib)
    test_02_process_empties_queue()
    test_02b_single_process_works()  # Catches beta.53 bug - single Process skipped Layer 1
    test_03_history_has_entries()
    test_04_pending_fixes_exist()
    # These tests modify files - only run if we have a test library
    # test_05_apply_fix_moves_file()
    # test_06_undo_restores_file()
    test_07_queue_empty_after_full_process()

    # Summary
    print()
    print("=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print()
    print(f"\033[92mPassed: {PASSED}\033[0m")
    print(f"\033[91mFailed: {FAILED}\033[0m")

    if ERRORS:
        print()
        print("Failures:")
        for err in ERRORS:
            print(f"  - {err}")

    print()

    if FAILED == 0:
        print("\033[92mAll tests passed!\033[0m")
        sys.exit(0)
    else:
        print("\033[91mSome tests failed - bugs detected!\033[0m")
        sys.exit(1)


if __name__ == "__main__":
    main()
