#!/usr/bin/env python3
"""
Comprehensive naming issue tests based on real GitHub issues.
Tests all edge cases reported by users to ensure they're handled correctly.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path

# Import app functions
from app import (
    clean_search_title,
    extract_series_from_title,
    build_new_path,
    is_garbage_match,
    is_placeholder_author,
    detect_multibook_vs_chapters,
    sanitize_path_component,
    analyze_full_path,
    AUDIO_EXTENSIONS
)

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
    print("NAMING ISSUE TESTS - Based on Real GitHub Issues")
    print("=" * 60)

    # ==========================================
    # Issue #33: Leading numbers in search
    # ==========================================
    print("\n--- Issue #33: Leading numbers stripped from search ---")

    tests = [
        ("06 - Blue Collar Space", "Blue Collar Space"),
        ("01 - Chapter One", "Chapter One"),
        ("12. The Beginning", "The Beginning"),
        ("Book 1 - Foundation", "Foundation"),  # Keep book indicator words
        ("1984", "1984"),  # Keep years/titles that ARE numbers
    ]

    for input_val, expected in tests:
        result = clean_search_title(input_val)
        if test_result(f"clean_search_title('{input_val}')", expected.lower() in result.lower(),
                      f"Expected '{expected}' in '{result}'"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Issue #29: Multibook false positives (chapter files)
    # ==========================================
    print("\n--- Issue #29: Chapter files NOT flagged as multibook ---")

    # Create fake file paths for testing
    chapter_files = [
        Path("/fake/book/00 - Prologue.mp3"),
        Path("/fake/book/01 - Chapter 1.mp3"),
        Path("/fake/book/02 - Chapter 2.mp3"),
        Path("/fake/book/03 - Part One.mp3"),
    ]

    result = detect_multibook_vs_chapters(chapter_files, {})
    if test_result("Chapter files (00-Prologue, 01-Chapter)",
                   result['is_multibook'] == False,
                   f"Got is_multibook={result['is_multibook']}, reason={result.get('reason')}"):
        passed += 1
    else:
        failed += 1

    # Real multibook files
    multibook_files = [
        Path("/fake/series/Book 1 - Foundation.mp3"),
        Path("/fake/series/Book 2 - Empire.mp3"),
        Path("/fake/series/Book 3 - Second.mp3"),
    ]

    result = detect_multibook_vs_chapters(multibook_files, {})
    if test_result("Multibook files (Book 1, Book 2, Book 3)",
                   result['is_multibook'] == True,
                   f"Got is_multibook={result['is_multibook']}"):
        passed += 1
    else:
        failed += 1

    # ==========================================
    # Issue #22 & #16: Empty series / dangling dash
    # ==========================================
    print("\n--- Issue #22 & #16: Template cleanup (no dangling dashes) ---")

    config = {
        'naming_format': 'custom',
        'custom_naming_template': '{author}/{series}/{series_num} - {title}',
        'series_grouping': True
    }

    lib_path = Path("/audiobooks")

    # No series - should NOT have dangling dash
    result = build_new_path(lib_path, "Barbara Truelove", "Of Monsters and Mainframes",
                           series=None, series_num=None, config=config)
    if result:
        path_str = str(result)
        if test_result("No series = no dash",
                       "- Of Monsters" not in path_str and "/- " not in path_str,
                       f"Got path: {path_str}"):
            passed += 1
        else:
            failed += 1
    else:
        failed += 1
        print(f"[FAIL] build_new_path returned None")

    # With series - should have proper format
    result = build_new_path(lib_path, "Brandon Sanderson", "The Final Empire",
                           series="Mistborn", series_num="1", config=config)
    if result:
        path_str = str(result)
        if test_result("With series = proper format",
                       "Mistborn" in path_str and "1 - " in path_str,
                       f"Got path: {path_str}"):
            passed += 1
        else:
            failed += 1
    else:
        failed += 1
        print(f"[FAIL] build_new_path returned None")

    # ==========================================
    # Issue #31: Messy folder names (year, quality, junk)
    # ==========================================
    print("\n--- Issue #31: Messy folder names cleaned ---")

    messy_names = [
        ("2018 - Blue Collar Space (multi) 128k {465mb}", "Blue Collar Space"),
        ("[bitsearch.to] Dean Koontz - Watchers", "Watchers"),
        ("The Martian Full Audiobook Unabridged", "The Martian"),
        ("Brandon Sanderson - Mistborn 01 - The Final Empire [MP3]", "The Final Empire"),
        ("audiobook_The_Great_Gatsby_full", "The Great Gatsby"),
        # Dates that ARE titles should NOT be stripped
        ("Stephen King - 11.22.63", "11.22.63"),  # Date IS the title
        ("1984", "1984"),  # Year IS the title
    ]

    for messy, expected_contains in messy_names:
        result = clean_search_title(messy)
        if test_result(f"Clean: '{messy[:40]}...'",
                       expected_contains.lower() in result.lower(),
                       f"Expected '{expected_contains}' in '{result}'"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Garbage match filtering
    # ==========================================
    print("\n--- Garbage Match Filtering ---")

    garbage_tests = [
        ("The Martian", "The Martin Chronicles", True),   # Too different
        ("The Martian", "The Martian", False),            # Exact match
        ("Foundation", "Foundation and Empire", False),   # Close enough (series)
        ("chapter1", "Gone Girl", True),                  # Unsearchable query
        ("1984", "Brave New World", True),                # Wrong book
    ]

    for original, suggested, should_reject in garbage_tests:
        result = is_garbage_match(original, suggested)
        if test_result(f"Garbage: '{original}' vs '{suggested}'",
                       result == should_reject,
                       f"Expected reject={should_reject}, got {result}"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Placeholder author detection
    # ==========================================
    print("\n--- Placeholder Author Detection ---")

    placeholder_tests = [
        ("Unknown", True),
        ("Various", True),
        ("Unknown Author", True),
        ("metadata", True),
        ("tmp", True),
        ("cache", True),
        ("Brandon Sanderson", False),
        ("Stephen King", False),
        ("J.K. Rowling", False),
    ]

    for author, should_be_placeholder in placeholder_tests:
        result = is_placeholder_author(author)
        if test_result(f"Placeholder: '{author}'",
                       result == should_be_placeholder,
                       f"Expected {should_be_placeholder}, got {result}"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Series extraction from title
    # ==========================================
    print("\n--- Series Extraction from Title ---")

    # Note: Series extraction only works for specific patterns like "Series #1" or "Series, Book 1"
    # Parenthetical series info like "(Mistborn Book 1)" is a different format not currently supported
    series_tests = [
        ("Foundation #1", ("Foundation", "1", "Foundation")),
        ("The Reckoners, Book 2 - Firefight", ("Reckoners", "2", "Firefight")),  # Comma-separated (strips "The")
        ("1984", (None, None, "1984")),  # No series
        ("Harry Potter and the Sorcerer's Stone", (None, None, "Harry Potter and the Sorcerer's Stone")),
    ]

    for title, expected in series_tests:
        series, num, clean_title = extract_series_from_title(title)
        expected_series, expected_num, expected_title = expected

        match = (series == expected_series and
                 str(num) == str(expected_num) if num and expected_num else num == expected_num)

        if test_result(f"Series from: '{title}'",
                       match,
                       f"Expected ({expected_series}, {expected_num}), got ({series}, {num})"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Path sanitization (Windows-safe)
    # ==========================================
    print("\n--- Path Sanitization ---")

    # Path sanitization removes Windows-illegal characters
    # Current behavior: removes rather than replaces (acceptable for safety)
    sanitize_tests = [
        ("The Book: A Story", "The Book A Story"),    # Colon removed (Windows-safe)
        ("What If?", "What If"),                       # Question mark removed
        ("Book <One>", "Book One"),                    # Angle brackets removed
        ("Part 1/2", "Part 12"),                       # Slash removed (path separator)
        ("  Spaces  ", "Spaces"),                      # Trimmed
    ]

    for input_val, expected in sanitize_tests:
        result = sanitize_path_component(input_val)
        if test_result(f"Sanitize: '{input_val}'",
                       result == expected,
                       f"Expected '{expected}', got '{result}'"):
            passed += 1
        else:
            failed += 1

    # ==========================================
    # Summary
    # ==========================================
    print("\n" + "=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
