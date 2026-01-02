#!/usr/bin/env python3
"""
UI Feature Tests - Verify templates and UI components work correctly.
Tests based on GitHub issues for UI-related features.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path

def test_result(name, passed, details=""):
    status = "\033[92mPASS\033[0m" if passed else "\033[91mFAIL\033[0m"
    print(f"[{status}] {name}")
    if details and not passed:
        print(f"       {details}")
    return passed

def main():
    passed = 0
    failed = 0

    print("=" * 60)
    print("UI FEATURE TESTS - Based on GitHub Issues")
    print("=" * 60)

    templates_dir = Path(__file__).parent.parent / "templates"

    # ==========================================
    # Issue #47: ABS Integration Explanation
    # ==========================================
    print("\n--- Issue #47: ABS Integration has explanation banner ---")

    abs_dashboard = templates_dir / "abs_dashboard.html"
    if abs_dashboard.exists():
        content = abs_dashboard.read_text()

        tests = [
            ("Info banner exists", "abs-info-banner" in content),
            ("Explains Progress Grid", "Progress Grid" in content),
            ("Explains Archive Candidates", "Archive Candidates" in content),
            ("Explains Untouched", "Untouched" in content),
            ("Explains User Groups", "User Groups" in content),
            ("Has dismissible functionality", "abs_info_dismissed" in content),
            ("Mentions Series Grouping tip", "Series Grouping" in content),
        ]

        for test_name, condition in tests:
            if test_result(f"abs_dashboard.html: {test_name}", condition):
                passed += 1
            else:
                failed += 1
    else:
        if test_result("abs_dashboard.html exists", False, f"File not found: {abs_dashboard}"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Issue #43: Tooltips on status badges
    # ==========================================
    print("\n--- Issue #43: Status badges have tooltips ---")

    library_html = templates_dir / "library.html"
    if library_html.exists():
        content = library_html.read_text()

        # Check for tooltip attributes on badges
        has_tooltips = 'title="' in content or "data-bs-toggle=\"tooltip\"" in content
        if test_result("library.html: Has tooltip attributes", has_tooltips):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Issue #42: Edit warning during processing
    # ==========================================
    print("\n--- Issue #42: Edit warning during queue processing ---")

    queue_html = templates_dir / "queue.html"
    if queue_html.exists():
        content = queue_html.read_text()

        # Check for processing warning
        has_warning = "processing" in content.lower() and ("warning" in content.lower() or "confirm" in content.lower())
        if test_result("queue.html: Has processing warning", has_warning):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Issue #53: Media type filter chips exist
    # ==========================================
    print("\n--- Issue #53: Media type filter exists in library ---")

    if library_html.exists():
        content = library_html.read_text()

        tests = [
            ("Audio Only filter exists", "audiobook_only" in content),
            ("Ebook Only filter exists", "ebook_only" in content),
            ("Both formats filter exists", "both_formats" in content),
            ("Filter count elements exist", "count-audiobook_only" in content),
        ]

        for test_name, condition in tests:
            if test_result(f"library.html: {test_name}", condition):
                passed += 1
            else:
                failed += 1

    # ==========================================
    # Issue #37: Multi-edit in queue
    # ==========================================
    print("\n--- Issue #37: Multi-edit exists in queue ---")

    queue_html = templates_dir / "queue.html"
    if queue_html.exists():
        content = queue_html.read_text()

        tests = [
            ("Multi-Edit button exists", "Multi-Edit" in content),
            ("Multi-edit modal exists", "multiEditModal" in content),
            ("openMultiEdit function exists", "function openMultiEdit" in content),
            ("saveAllMultiEdits function exists", "function saveAllMultiEdits" in content),
            ("Search per item exists", "searchForMultiItem" in content),
            ("Modified count tracking", "updateModifiedCount" in content),
        ]

        for test_name, condition in tests:
            if test_result(f"queue.html: {test_name}", condition):
                passed += 1
            else:
                failed += 1

    # ==========================================
    # Issue #53: detect_media_type function works
    # ==========================================
    print("\n--- Issue #53: detect_media_type function ---")

    try:
        from app import detect_media_type, AUDIO_EXTENSIONS, EBOOK_EXTENSIONS
        import tempfile
        import os

        # Test with a temp directory containing audio files
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create audio file
            audio_file = os.path.join(tmpdir, "test.mp3")
            open(audio_file, 'w').close()

            result = detect_media_type(tmpdir)
            if test_result("Folder with audio = 'audiobook'", result == 'audiobook'):
                passed += 1
            else:
                failed += 1

        # Test with ebook file
        with tempfile.TemporaryDirectory() as tmpdir:
            ebook_file = os.path.join(tmpdir, "test.epub")
            open(ebook_file, 'w').close()

            result = detect_media_type(tmpdir)
            if test_result("Folder with ebook = 'ebook'", result == 'ebook'):
                passed += 1
            else:
                failed += 1

        # Test with both
        with tempfile.TemporaryDirectory() as tmpdir:
            audio_file = os.path.join(tmpdir, "test.mp3")
            ebook_file = os.path.join(tmpdir, "test.epub")
            open(audio_file, 'w').close()
            open(ebook_file, 'w').close()

            result = detect_media_type(tmpdir)
            if test_result("Folder with both = 'both'", result == 'both'):
                passed += 1
            else:
                failed += 1

    except Exception as e:
        if test_result(f"detect_media_type import", False, str(e)):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Summary
    # ==========================================
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"RESULTS: {passed}/{total} passed ({100*passed//total if total > 0 else 0}%)")

    if failed > 0:
        print(f"\033[91m{failed} tests failed!\033[0m")
        return 1
    else:
        print("\033[92mAll tests passed!\033[0m")
        return 0

if __name__ == "__main__":
    sys.exit(main())
