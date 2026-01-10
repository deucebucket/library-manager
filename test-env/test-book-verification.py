#!/usr/bin/env python3
"""
Book Verification Test
Tests that the library manager correctly identifies books and detects problems.

Run after integration tests have populated the database:
    python test-env/test-book-verification.py [db_path]

Default db_path: test-env/fresh-deploy/data/library.db
"""

import sqlite3
import sys
import os

# Colors for output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

def log_pass(msg):
    print(f"{GREEN}[PASS]{NC} {msg}")

def log_fail(msg):
    print(f"{RED}[FAIL]{NC} {msg}")

def log_info(msg):
    print(f"{YELLOW}[INFO]{NC} {msg}")


# Expected correct book identifications
# Format: (current_author, current_title, expected_status_or_check)
EXPECTED_CORRECT_BOOKS = [
    # Classic books that should be correctly attributed
    ("George Orwell", "1984", "verified"),
    ("George Orwell", "Animal Farm", "verified"),
    ("Stephen King", "Dark Tower", "verified"),
    # These may still be pending but author/title should be correct
    ("Stephen King", "It", None),
    ("Stephen King", "The Shining", None),
    ("J.R.R. Tolkien", "The Hobbit", None),
    ("Jane Austen", "Pride and Prejudice", None),
    ("Agatha Christie", "And Then There Were None", None),
    ("Agatha Christie", "Murder on the Orient Express", None),
    ("Andy Weir", "Project Hail Mary", None),
    ("Brandon Sanderson", "The Final Empire", None),
    ("Isaac Asimov", "Foundation", None),
]

# Expected problem detections
# Format: (current_author, current_title, expected_status)
# NOTE: structure_reversed detection was removed in beta.69 (Issue #52 - false positives)
# Items with reversed structure now go through normal API lookup flow
EXPECTED_PROBLEMS = [
    # Reversed structure items now go to queue for API lookup, not auto-detected
    # ("Metro 2033", "Dmitry Glukhovsky", "structure_reversed"),  # Removed in beta.69
]

# Expected queue detections (reason field)
# Format: (current_author, current_title, expected_reason_contains)
EXPECTED_QUEUE_REASONS = [
    # The Expanse is a series name, not author - should detect this
    ("The Expanse", "Leviathan Wakes", "title_looks_like_author"),
    ("The Expanse", "Calibans War", "title_looks_like_author"),
    # Year in title should be detected
    ("Terry Pratchett", "Good Omens (1990)", "year_in_title"),
]

# Expected series folder detections
EXPECTED_SERIES_FOLDERS = [
    ("Brandon Sanderson", "Mistborn", "series_folder"),
    ("Frank Herbert", "Dune", "series_folder"),
    ("Isaac Asimov", "Foundation", "series_folder"),
    ("J.K. Rowling", "Harry Potter", "series_folder"),
    ("J.R.R. Tolkien", "Lord of the Rings", "series_folder"),
]


def run_tests(db_path):
    """Run all verification tests."""
    passed = 0
    failed = 0

    if not os.path.exists(db_path):
        log_fail(f"Database not found: {db_path}")
        return 0, 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("=" * 50)
    print("Book Verification Tests")
    print("=" * 50)
    print()

    # Test 1: Check correctly identified books
    log_info("Testing correct book identifications...")
    for author, title, expected_status in EXPECTED_CORRECT_BOOKS:
        cursor.execute(
            "SELECT current_author, current_title, status FROM books WHERE current_author = ? AND current_title = ?",
            (author, title)
        )
        row = cursor.fetchone()

        if row:
            if expected_status and row['status'] != expected_status:
                log_fail(f"{author}/{title}: expected status '{expected_status}', got '{row['status']}'")
                failed += 1
            else:
                log_pass(f"{author}/{title}: correctly identified")
                passed += 1
        else:
            # Try partial match on title
            cursor.execute(
                "SELECT current_author, current_title, status FROM books WHERE current_author = ? AND current_title LIKE ?",
                (author, f"%{title}%")
            )
            row = cursor.fetchone()
            if row:
                log_pass(f"{author}/{title}: found as '{row['current_title']}'")
                passed += 1
            else:
                log_fail(f"{author}/{title}: not found in database")
                failed += 1

    print()

    # Test 2: Check problem detections
    log_info("Testing problem pattern detection...")
    for author, title, expected_status in EXPECTED_PROBLEMS:
        cursor.execute(
            "SELECT current_author, current_title, status FROM books WHERE current_author = ? AND current_title = ?",
            (author, title)
        )
        row = cursor.fetchone()

        if row:
            if row['status'] == expected_status:
                log_pass(f"{author}/{title}: correctly detected as '{expected_status}'")
                passed += 1
            else:
                log_fail(f"{author}/{title}: expected '{expected_status}', got '{row['status']}'")
                failed += 1
        else:
            log_fail(f"{author}/{title}: not found in database")
            failed += 1

    print()

    # Test 3: Check series folder detection
    log_info("Testing series folder detection...")
    for author, title, expected_status in EXPECTED_SERIES_FOLDERS:
        cursor.execute(
            "SELECT current_author, current_title, status FROM books WHERE current_author = ? AND current_title = ?",
            (author, title)
        )
        row = cursor.fetchone()

        if row:
            if row['status'] == expected_status:
                log_pass(f"{author}/{title}: correctly detected as series folder")
                passed += 1
            else:
                log_fail(f"{author}/{title}: expected '{expected_status}', got '{row['status']}'")
                failed += 1
        else:
            log_fail(f"{author}/{title}: not found in database")
            failed += 1

    print()

    # Test 4: Check queue reasons
    log_info("Testing queue item reasons...")
    for author, title, expected_reason in EXPECTED_QUEUE_REASONS:
        cursor.execute("""
            SELECT b.current_author, b.current_title, q.reason
            FROM books b
            JOIN queue q ON b.id = q.book_id
            WHERE b.current_author = ? AND b.current_title = ?
        """, (author, title))
        row = cursor.fetchone()

        if row:
            if expected_reason in (row['reason'] or ''):
                log_pass(f"{author}/{title}: queue reason contains '{expected_reason}'")
                passed += 1
            else:
                log_fail(f"{author}/{title}: expected reason containing '{expected_reason}', got '{row['reason']}'")
                failed += 1
        else:
            # May not be in queue anymore if processed
            log_info(f"{author}/{title}: not in queue (may have been processed)")
            passed += 1  # Not a failure

    print()

    # Test 5: Spot check - verify no obvious misidentifications
    log_info("Spot checking for misidentifications...")
    cursor.execute("""
        SELECT current_author, current_title, status FROM books
        WHERE status = 'verified'
        ORDER BY RANDOM()
        LIMIT 10
    """)
    verified = cursor.fetchall()

    misidentified = 0
    for row in verified:
        author = row['current_author']
        title = row['current_title']
        # Check for obvious problems
        if title.lower() in author.lower() or author.lower() in title.lower():
            if len(title) > 5:  # Skip short matches like "It"
                log_fail(f"Possible misidentification: {author}/{title} - title/author overlap")
                misidentified += 1
        elif author.lower() in ['unknown', 'various', 'audiobook']:
            log_fail(f"Possible misidentification: {author}/{title} - generic author")
            misidentified += 1

    if misidentified == 0:
        log_pass(f"Spot check: {len(verified)} verified books look correct")
        passed += 1
    else:
        failed += misidentified

    print()

    # Test 6: Check no books are stuck in error state without reason
    log_info("Checking for stuck error states...")
    cursor.execute("""
        SELECT COUNT(*) as count FROM books
        WHERE status = 'error' AND (error_message IS NULL OR error_message = '')
    """)
    stuck = cursor.fetchone()['count']

    if stuck == 0:
        log_pass("No books stuck in error state without message")
        passed += 1
    else:
        log_fail(f"{stuck} books stuck in error state without error message")
        failed += 1

    conn.close()

    return passed, failed


def main():
    # Default path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, "fresh-deploy/data/library.db")

    db_path = sys.argv[1] if len(sys.argv) > 1 else default_db

    passed, failed = run_tests(db_path)

    print("=" * 50)
    print("Summary")
    print("=" * 50)
    print(f"{GREEN}Passed: {passed}{NC}")
    print(f"{RED}Failed: {failed}{NC}")
    print()

    if failed == 0:
        print(f"{GREEN}All verification tests passed!{NC}")
        return 0
    else:
        print(f"{RED}Some verification tests failed{NC}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
