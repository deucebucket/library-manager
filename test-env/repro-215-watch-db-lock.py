#!/usr/bin/env python3
"""Reproduce Issue #215: watch-folder worker hits 'database is locked'.

Scenario A (as reported): a scan-style connection holds a write transaction
longer than the 30s busy_timeout while watch_folder_mark_processed tries to
write through its own fresh connection.

Scenario B (self-contention): the watch worker's own connection has an
uncommitted write (e.g. a books insert whose commit never ran because an
earlier statement raised), then watch_folder_mark_processed opens a SECOND
connection which stalls against the worker connection's own lock.

Post-fix check: passing the worker connection into watch_folder_mark_processed
makes scenario B succeed instantly.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
import tempfile
import threading
import time

import library_manager.database as db


def fresh_db():
    fd, path = tempfile.mkstemp(suffix='.db', prefix='repro215_')
    os.close(fd)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('''CREATE TABLE watch_folder_processed (
        path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        outcome TEXT,
        error_message TEXT)''')
    conn.execute('''CREATE TABLE books (id INTEGER PRIMARY KEY, path TEXT UNIQUE,
        current_author TEXT, current_title TEXT, status TEXT)''')
    conn.commit()
    conn.close()
    return path


def scenario_a():
    print("=" * 70)
    print("SCENARIO A: scan worker holds write lock > 30s, watch marks processed")
    print("=" * 70)
    path = fresh_db()
    stop = threading.Event()

    def scan_worker():
        conn = sqlite3.connect(path, timeout=30)
        conn.execute('PRAGMA busy_timeout=30000')
        conn.execute('BEGIN IMMEDIATE')
        conn.execute("INSERT INTO books (path, status) VALUES ('/scan/item', 'scanning')")
        # Simulate a long scan-layer transaction (~33s > 30s busy_timeout)
        while not stop.is_set():
            time.sleep(0.1)
        conn.rollback()
        conn.close()

    t = threading.Thread(target=scan_worker)
    t.start()
    time.sleep(0.5)  # let scan grab the write lock

    start = time.time()
    try:
        db.watch_folder_mark_processed('/watch/Test Author - Book', 'moved', db_path=path)
        print(f"  mark_processed SUCCEEDED after {time.time()-start:.1f}s")
        result = False
    except sqlite3.OperationalError as e:
        elapsed = time.time() - start
        print(f"  mark_processed FAILED after {elapsed:.1f}s: {e}")
        result = 'locked' in str(e)
    finally:
        stop.set()
        t.join()
    os.unlink(path)
    return result


def scenario_b(use_shared_conn=False):
    label = "POST-FIX (shared conn)" if use_shared_conn else "PRE-FIX (fresh conn per call)"
    print("=" * 70)
    print(f"SCENARIO B {label}: worker conn holds uncommitted write")
    print("=" * 70)
    path = fresh_db()

    worker = sqlite3.connect(path, timeout=30)
    worker.execute('PRAGMA busy_timeout=30000')
    # Worker wrote a books row but its commit never happened (exception path
    # in process_watch_folder skips conn.commit(), leaving the tx open)
    worker.execute("INSERT INTO books (path, status) VALUES ('/watch/pending-item', 'pending')")

    start = time.time()
    try:
        if use_shared_conn:
            db.watch_folder_mark_processed('/watch/item-b', 'moved', db_path=path, conn=worker)
        else:
            db.watch_folder_mark_processed('/watch/item-b', 'moved', db_path=path)
        print(f"  mark_processed SUCCEEDED after {time.time()-start:.1f}s")
        outcome = False
    except (sqlite3.OperationalError, TypeError) as e:
        elapsed = time.time() - start
        print(f"  mark_processed FAILED after {elapsed:.1f}s: {e}")
        outcome = 'locked' in str(e)
    finally:
        worker.rollback()
        worker.close()
    os.unlink(path)
    return outcome


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else 'all'
    a = scenario_a() if mode in ('all', 'a') else None
    b_pre = scenario_b(use_shared_conn=False) if mode in ('all', 'b') else None
    b_post = scenario_b(use_shared_conn=True) if mode in ('all', 'b', 'fix') else None

    print()
    print("=" * 70)
    if a is not None:
        print(f"A cross-worker lock timeout reproduced: {'YES (bug present)' if a else 'no'}")
    if b_pre is not None:
        print(f"B self-contention reproduced: {'YES (bug present)' if b_pre else 'no'}")
    if b_post is not None:
        print(f"B with shared-conn fix: {'STILL FAILING' if b_post else 'fixed (succeeded)'}")
