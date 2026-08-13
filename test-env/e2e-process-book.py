"""End-to-end processing test inside the quarantined container.

Copies a known-good test audiobook to a messy folder, triggers a library scan,
waits for the pipeline to identify it, and verifies that:
- book_id (ASIN) is populated (#273)
- detected_language is set (#275)
- WWWAUDIOFILE is embedded in the audio file (#274)
"""
import json
import shutil
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

BASE_URL = "http://127.0.0.1:5757"
LIBRARY_PATH = Path("/opt/test-library")
SOURCE_BOOK = LIBRARY_PATH / "Frank Herbert" / "Dune (Scott Brick) (ENG)" / "audiobook.mp3"
TEST_FOLDER = LIBRARY_PATH / "[ messy ] Herbert, Frank - Doon (audio)"
TEST_AUDIO = TEST_FOLDER / "audiobook.mp3"
CANONICAL_FOLDER = LIBRARY_PATH / "Frank Herbert" / "Dune (Scott Brick) (ENG)"
CANONICAL_BACKUP = LIBRARY_PATH / "Frank Herbert" / "Dune (Scott Brick) (ENG).backup"
DB_PATH = Path("/opt/library-manager/library.db")
MAX_WAIT_SECONDS = 300


def api_scan():
    """Trigger a library scan via the API."""
    req = urllib.request.Request(f"{BASE_URL}/api/scan", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[WARN] Scan API call failed: {e}")
        return False


def api_process_background():
    """Start background processing so scanned books are identified."""
    req = urllib.request.Request(f"{BASE_URL}/api/process_background", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[WARN] Process background API call failed: {e}")
        return False


def wait_for_book_state(status_targets, timeout=MAX_WAIT_SECONDS):
    """Poll the DB until the test book (matched by source path) reaches one of the target statuses."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    started = time.time()
    while time.time() - started < timeout:
        c.execute("SELECT id, current_author, current_title, status, verification_layer, profile FROM books WHERE path LIKE ?", (str(TEST_FOLDER) + '%',))
        row = c.fetchone()
        if row and row['status'] in status_targets:
            conn.close()
            return dict(row)
        time.sleep(5)
    conn.close()
    return None


def main():
    failures = []

    # 1. Prepare messy test book folder
    if TEST_FOLDER.exists():
        shutil.rmtree(TEST_FOLDER)
    TEST_FOLDER.mkdir(parents=True)
    shutil.copy2(SOURCE_BOOK, TEST_AUDIO)

    # Strip existing tags so we can prove tags were embedded by this pipeline run.
    from mutagen.mp3 import MP3
    audio = MP3(str(TEST_AUDIO))
    if audio.tags:
        audio.tags.clear()
        audio.save()
    print(f"[INFO] Copied and stripped test audio to {TEST_AUDIO}")

    # 2. Remove any previous test book record and the existing canonical record
    # so the messy copy can be fixed into the canonical path without a DB collision.
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM books WHERE path LIKE ?", (str(CANONICAL_FOLDER) + '%',))
    canonical_row = c.fetchone()
    canonical_book_id = canonical_row[0] if canonical_row else None
    if canonical_book_id:
        c.execute("DELETE FROM history WHERE book_id = ?", (canonical_book_id,))
        c.execute("DELETE FROM queue WHERE book_id = ?", (canonical_book_id,))
        c.execute("DELETE FROM books WHERE id = ?", (canonical_book_id,))
    c.execute("DELETE FROM books WHERE path LIKE ?", (str(TEST_FOLDER) + '%',))
    c.execute("DELETE FROM history WHERE old_path LIKE ? OR new_path LIKE ?", (str(TEST_FOLDER) + '%',) * 2)
    conn.commit()
    conn.close()
    print("[INFO] Cleaned up previous and canonical test records")

    # 3. Trigger scan
    print("[INFO] Triggering library scan...")
    api_scan()

    # 3b. Start background processing (the worker sleeps for hours by default,
    # so the test must explicitly kick off processing after the scan).
    print("[INFO] Starting background processing...")
    api_process_background()

    # 4. Wait for book to be identified and pending fix (or verified if already correct)
    print("[INFO] Waiting for book to reach pending_fix or verified...")
    book = wait_for_book_state({"pending_fix", "verified"})
    if not book:
        failures.append("Book did not reach pending_fix or verified within timeout")
        print("\n".join(failures))
        sys.exit(1)

    print(f"[INFO] Book state: {book['status']} (layer {book['verification_layer']})")
    profile = json.loads(book['profile'] or '{}')

    # 5. Verify ASIN persistence (#273)
    book_id = profile.get('book_id')
    if book_id:
        print(f"[PASS] book_id persisted: {book_id}")
    else:
        failures.append(f"book_id missing from profile: {profile}")

    # 6. Verify detected language (#275)
    detected = profile.get('detected_language') or (profile.get('language') or {}).get('value')
    if detected:
        print(f"[PASS] detected_language persisted: {detected}")
    else:
        failures.append(f"detected_language missing from profile: {profile}")

    # 7. Apply the fix via API. To avoid a destination collision with the
    # existing canonical copy, temporarily rename it out of the way and
    # delete any DB records the background scanner may have created for it.
    print("[INFO] Applying fix...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM history WHERE book_id = ? AND status = 'pending_fix' ORDER BY id DESC LIMIT 1", (book['id'],))
    hist_row = c.fetchone()
    conn.close()

    audio_file_to_check = None
    backup_made = False
    if hist_row:
        # Remove any DB records pointing to the canonical path (the background
        # scanner may have re-added the original while we were waiting).
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id FROM books WHERE path LIKE ?", (str(CANONICAL_FOLDER) + '%',))
        for canonical_id_row in c.fetchall():
            canonical_id = canonical_id_row[0]
            if canonical_id == book['id']:
                continue
            c.execute("DELETE FROM history WHERE book_id = ?", (canonical_id,))
            c.execute("DELETE FROM queue WHERE book_id = ?", (canonical_id,))
            c.execute("DELETE FROM books WHERE id = ?", (canonical_id,))
            print(f"[INFO] Removed scanner-added canonical book record {canonical_id}")
        conn.commit()
        conn.close()

        if CANONICAL_FOLDER.exists():
            if CANONICAL_BACKUP.exists():
                shutil.rmtree(CANONICAL_BACKUP)
            shutil.move(str(CANONICAL_FOLDER), str(CANONICAL_BACKUP))
            backup_made = True
            print(f"[INFO] Renamed existing canonical folder to {CANONICAL_BACKUP}")

        history_id = hist_row[0]
        req = urllib.request.Request(
            f"{BASE_URL}/api/apply_fix/{history_id}",
            method="POST",
            data=b"",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read().decode())
                print(f"[INFO] apply_fix result: {result}")
                if result.get('success'):
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute("SELECT path FROM books WHERE id = ?", (book['id'],))
                    row = c.fetchone()
                    conn.close()
                    if row and row[0]:
                        final_path = Path(row[0])
                        if final_path.exists():
                            audio_file_to_check = final_path / "audiobook.mp3"
                else:
                    failures.append(f"apply_fix failed: {result}")
        except Exception as e:
            failures.append(f"apply_fix failed: {e}")
    else:
        # Already verified / nothing to fix
        print("[INFO] No pending_fix history item; book may already be correct")

    # 8. Verify WWWAUDIOFILE tag (#274)
    if audio_file_to_check and not audio_file_to_check.exists():
        audio_files = list(audio_file_to_check.parent.glob("*.mp3")) + list(audio_file_to_check.parent.glob("*.m4b"))
        audio_file_to_check = audio_files[0] if audio_files else None
    if audio_file_to_check:
        from mutagen.mp3 import MP3
        tags = MP3(str(audio_file_to_check))
        if "TXXX:WWWAUDIOFILE" in tags:
            url = str(tags["TXXX:WWWAUDIOFILE"])
            print(f"[PASS] WWWAUDIOFILE embedded: {url}")
            if book_id and book_id not in url:
                failures.append(f"WWWAUDIOFILE URL does not contain book_id {book_id}: {url}")
        else:
            failures.append(f"TXXX:WWWAUDIOFILE missing from {audio_file_to_check}")
    else:
        failures.append("No audio file found to verify tags")

    # 9. Restore the original canonical copy and remove the test record
    if backup_made and CANONICAL_BACKUP.exists():
        # Remove the test book record so the canonical path is free
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id FROM books WHERE path LIKE ?", (str(CANONICAL_FOLDER) + '%',))
        test_canonical_row = c.fetchone()
        if test_canonical_row:
            test_id = test_canonical_row[0]
            c.execute("DELETE FROM history WHERE book_id = ?", (test_id,))
            c.execute("DELETE FROM queue WHERE book_id = ?", (test_id,))
            c.execute("DELETE FROM books WHERE id = ?", (test_id,))
        conn.commit()
        conn.close()

        if CANONICAL_FOLDER.exists():
            shutil.rmtree(CANONICAL_FOLDER)
        shutil.move(str(CANONICAL_BACKUP), str(CANONICAL_FOLDER))
        print(f"[INFO] Restored original canonical folder from {CANONICAL_BACKUP}")

    if TEST_FOLDER.exists():
        shutil.rmtree(TEST_FOLDER)
        print(f"[INFO] Cleaned up test folder {TEST_FOLDER}")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nEnd-to-end processing test passed.")


if __name__ == "__main__":
    main()
