#!/usr/bin/env python3
"""Regression tests for verified apply-fix transfers and rollback receipts."""

import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import app as app_module
from library_manager import database
from library_manager.file_transactions import (
    build_file_inventory,
    inventory_digest,
    recover_interrupted_apply_fixes,
    rollback_filesystem_move,
    store_inventory,
)


def test_config(library_root):
    return {
        'library_paths': [str(library_root)],
        'watch_folder': '',
        'watch_output_folder': '',
        'metadata_embedding_enabled': False,
    }


def create_case(base, source_is_file=False, destination_exists=False):
    library_root = base / 'library'
    library_root.mkdir()
    if source_is_file:
        source = library_root / 'Old Author' / 'Old Title.m4b'
        source.parent.mkdir(parents=True)
        source.write_bytes(b'original single-file audiobook\x00\x01')
    else:
        source = library_root / 'Old Author' / 'Old Title'
        source.mkdir(parents=True)
        (source / 'Part 01.mp3').write_bytes(b'first verified track\x00\x01')
        extras = source / 'Extras'
        extras.mkdir()
        (extras / 'cover.jpg').write_bytes(b'cover-image-bytes\x02\x03')

    destination = library_root / 'New Author' / 'New Title'
    if destination_exists:
        destination.mkdir(parents=True)

    db_path = base / 'library.db'
    database.set_db_path(str(db_path))
    database.init_db(str(db_path))
    conn = database.get_db()
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO books
                      (path, current_author, current_title, status, source_type)
                      VALUES (?, 'Old Author', 'Old Title', 'pending_fix', 'library')''',
                   (str(source),))
    book_id = cursor.lastrowid
    cursor.execute("INSERT INTO queue (book_id, reason) VALUES (?, 'test')", (book_id,))
    cursor.execute('''INSERT INTO history
                      (book_id, old_author, old_title, new_author, new_title,
                       old_path, new_path, status)
                      VALUES (?, 'Old Author', 'Old Title', 'New Author', 'New Title',
                              ?, ?, 'pending_fix')''',
                   (book_id, str(source), str(destination)))
    history_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {
        'library_root': library_root,
        'db_path': db_path,
        'source': source,
        'destination': destination,
        'book_id': book_id,
        'history_id': history_id,
        'destination_exists': destination_exists,
    }


def run_apply(case):
    original_load_config = app_module.load_config
    original_run_hooks = app_module.run_hooks
    original_load_secrets = app_module.load_secrets
    app_module.load_config = lambda: test_config(case['library_root'])
    app_module.run_hooks = lambda *args, **kwargs: None
    app_module.load_secrets = lambda: {}
    try:
        return app_module.apply_fix(case['history_id'])
    finally:
        app_module.load_config = original_load_config
        app_module.run_hooks = original_run_hooks
        app_module.load_secrets = original_load_secrets


def fetch_receipt(case):
    conn = database.get_db()
    operation = conn.execute(
        'SELECT * FROM file_operations WHERE history_id = ? ORDER BY id DESC LIMIT 1',
        (case['history_id'],),
    ).fetchone()
    inventory = []
    if operation:
        inventory = conn.execute(
            '''SELECT phase, relative_path, entry_type, size, sha256, verified
               FROM file_operation_inventory WHERE operation_id = ?
               ORDER BY phase, relative_path''',
            (operation['id'],),
        ).fetchall()
    conn.close()
    return operation, inventory


def assert_failed_database_state(case):
    conn = database.get_db()
    book = conn.execute('SELECT * FROM books WHERE id = ?', (case['book_id'],)).fetchone()
    history = conn.execute('SELECT * FROM history WHERE id = ?', (case['history_id'],)).fetchone()
    queue_count = conn.execute('SELECT COUNT(*) FROM queue WHERE book_id = ?',
                               (case['book_id'],)).fetchone()[0]
    conn.close()
    assert book['path'] == str(case['source'])
    assert book['current_author'] == 'Old Author'
    assert book['current_title'] == 'Old Title'
    assert book['status'] == 'pending_fix'
    assert history['status'] == 'error'
    assert queue_count == 1
    return history


def test_folder_db_failure_restores_verified_inventory():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir), destination_exists=True)
        source_receipt = build_file_inventory(case['source'], [case['library_root']])
        conn = database.get_db()
        conn.execute(f'''CREATE TRIGGER fail_apply_path_update
                         BEFORE UPDATE OF path ON books
                         WHEN OLD.id = {case['book_id']}
                         BEGIN
                           SELECT RAISE(ABORT, 'forced books.path update failure');
                         END''')
        conn.commit()
        conn.close()

        success, message = run_apply(case)
        assert not success
        assert 'forced books.path update failure' in message
        assert 'original inventory verified' in message
        assert case['source'].is_dir()
        assert build_file_inventory(case['source'], [case['library_root']]) == source_receipt
        assert case['destination'].is_dir()
        assert list(case['destination'].iterdir()) == []
        assert not (case['destination'].parent / 'New Title_temp').exists()
        history = assert_failed_database_state(case)
        assert 'MANUAL RECOVERY REQUIRED' not in history['error_message']

        operation, inventory = fetch_receipt(case)
        assert operation['status'] == 'rolled_back'
        assert operation['source_digest'] == operation['destination_digest']
        assert operation['source_digest'] == operation['rollback_digest']
        assert {row['phase'] for row in inventory} == {'source', 'destination', 'rollback'}
        assert all(row['verified'] == 1 for row in inventory)
    print('[PASS] folder DB failure restores and verifies the complete source inventory')


def test_stale_unique_path_collision_is_blocked_before_move():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        conn = database.get_db()
        conn.execute('''INSERT INTO books
                        (path, current_author, current_title, status, source_type)
                        VALUES (?, 'Stale Author', 'Stale Record', 'error', 'library')''',
                     (str(case['destination']),))
        conn.commit()
        conn.close()

        source_receipt = build_file_inventory(case['source'], [case['library_root']])
        success, message = run_apply(case)
        assert not success
        assert 'Destination path is already assigned to book' in message
        assert case['source'].exists()
        assert not case['destination'].exists()
        assert build_file_inventory(case['source'], [case['library_root']]) == source_receipt
        assert_failed_database_state(case)
        operation, inventory = fetch_receipt(case)
        assert operation is None
        assert inventory == []
    print('[PASS] stale UNIQUE path collision is rejected before filesystem handoff')


def test_case_variant_database_collision_is_blocked_before_move():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        case_variant = str(case['destination']).replace('New Author', 'new author')
        conn = database.get_db()
        conn.execute('''INSERT INTO books
                        (path, current_author, current_title, status, source_type)
                        VALUES (?, 'Stale Author', 'Stale Record', 'error', 'library')''',
                     (case_variant,))
        conn.commit()
        conn.close()

        source_receipt = build_file_inventory(case['source'], [case['library_root']])
        success, message = run_apply(case)
        assert not success
        assert 'Destination path is already assigned to book' in message
        assert build_file_inventory(case['source'], [case['library_root']]) == source_receipt
        assert not case['destination'].exists()
        operation, _ = fetch_receipt(case)
        assert operation is None
    print('[PASS] case-variant database collision is rejected before filesystem handoff')


def test_configured_root_can_never_be_moved():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        conn = database.get_db()
        conn.execute('UPDATE books SET path = ? WHERE id = ?',
                     (str(case['library_root']), case['book_id']))
        conn.execute('UPDATE history SET old_path = ? WHERE id = ?',
                     (str(case['library_root']), case['history_id']))
        conn.commit()
        conn.close()

        success, message = run_apply(case)
        assert not success
        assert 'Refusing to move a configured root' in message
        assert case['library_root'].is_dir()
        assert case['source'].is_dir()
        operation, _ = fetch_receipt(case)
        assert operation is None
    print('[PASS] configured library roots can never become an apply source')


def test_single_file_db_failure_restores_exact_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir), source_is_file=True)
        original_bytes = case['source'].read_bytes()
        conn = database.get_db()
        conn.execute(f'''CREATE TRIGGER fail_single_file_path_update
                         BEFORE UPDATE OF path ON books
                         WHEN OLD.id = {case['book_id']}
                         BEGIN
                           SELECT RAISE(ABORT, 'forced single-file update failure');
                         END''')
        conn.commit()
        conn.close()

        success, message = run_apply(case)
        assert not success
        assert 'original inventory verified' in message
        assert case['source'].is_file()
        assert case['source'].read_bytes() == original_bytes
        assert not (case['destination'] / case['source'].name).exists()
        assert_failed_database_state(case)
        operation, inventory = fetch_receipt(case)
        assert operation['destination_path'].endswith(case['source'].name)
        assert operation['status'] == 'rolled_back'
        assert all(row['verified'] == 1 for row in inventory)
    print('[PASS] single-file DB failure restores the exact file and receipt')


def test_destination_mismatch_is_rejected_and_rolled_back():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        real_inventory = app_module.build_file_inventory
        calls = {'count': 0}

        def mismatched_destination(path, roots):
            calls['count'] += 1
            inventory = real_inventory(path, roots)
            if calls['count'] == 2:
                inventory = [dict(item) for item in inventory]
                file_entry = next(item for item in inventory if item['entry_type'] == 'file')
                file_entry['sha256'] = '0' * 64
            return inventory

        with patch.object(app_module, 'build_file_inventory', side_effect=mismatched_destination):
            success, message = run_apply(case)

        assert not success
        assert 'Destination inventory does not match source receipt' in message
        assert case['source'].exists()
        assert not case['destination'].exists()
        assert_failed_database_state(case)
        operation, inventory = fetch_receipt(case)
        assert operation['status'] == 'rolled_back'
        destination_rows = [row for row in inventory if row['phase'] == 'destination']
        assert any(row['verified'] == 0 for row in destination_rows)
    print('[PASS] destination hash mismatch blocks commit and restores source')


def test_rollback_failure_requires_manual_recovery():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        conn = database.get_db()
        conn.execute(f'''CREATE TRIGGER fail_for_manual_recovery
                         BEFORE UPDATE OF path ON books
                         WHEN OLD.id = {case['book_id']}
                         BEGIN
                           SELECT RAISE(ABORT, 'forced rollback test failure');
                         END''')
        conn.commit()
        conn.close()

        with patch.object(app_module, 'rollback_filesystem_move',
                          return_value=(False, 'simulated rollback failure')):
            success, message = run_apply(case)

        assert not success
        assert 'MANUAL RECOVERY REQUIRED' in message
        assert str(case['source']) in message
        assert str(case['destination']) in message
        history = assert_failed_database_state(case)
        assert 'MANUAL RECOVERY REQUIRED' in history['error_message']
        operation, _ = fetch_receipt(case)
        assert operation['status'] == 'rollback_failed'
    print('[PASS] rollback failure records exact manual-recovery paths')


def test_success_commits_only_after_verified_handoff():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        success, message = run_apply(case)
        assert success, message
        assert not case['source'].exists()
        assert case['destination'].is_dir()

        conn = database.get_db()
        book = conn.execute('SELECT * FROM books WHERE id = ?', (case['book_id'],)).fetchone()
        history = conn.execute('SELECT * FROM history WHERE id = ?', (case['history_id'],)).fetchone()
        queue_count = conn.execute('SELECT COUNT(*) FROM queue WHERE book_id = ?',
                                   (case['book_id'],)).fetchone()[0]
        conn.close()
        assert book['path'] == str(case['destination'])
        assert book['status'] == 'fixed'
        assert history['status'] == 'fixed'
        assert queue_count == 0

        operation, inventory = fetch_receipt(case)
        assert operation['status'] == 'committed'
        assert operation['source_digest'] == operation['destination_digest']
        assert operation['rollback_digest'] is None
        assert {row['phase'] for row in inventory} == {'source', 'destination'}
        assert all(row['verified'] == 1 for row in inventory)

        client = app_module.app.test_client()
        response = client.get(f"/api/file-operation-receipt/{operation['id']}")
        payload = response.get_json()
        assert response.status_code == 200
        assert payload['success'] is True
        assert payload['operation']['status'] == 'committed'
        assert len(payload['inventory']) == len(inventory)
        history_response = client.get('/history')
        assert history_response.status_code == 200
        assert b'View transfer receipt' in history_response.data
        assert b'Committed' in history_response.data
    print('[PASS] successful move commits only after verified handoff and exposes receipt')


def test_successful_folder_and_file_moves_remain_undoable():
    for source_is_file in (False, True):
        with tempfile.TemporaryDirectory() as temp_dir:
            case = create_case(Path(temp_dir), source_is_file=source_is_file)
            success, message = run_apply(case)
            assert success, message
            operation, _ = fetch_receipt(case)
            exact_destination = Path(operation['destination_path'])
            assert exact_destination.exists()

            with patch.object(app_module, 'load_config',
                              return_value=test_config(case['library_root'])):
                response = app_module.app.test_client().post(
                    f"/api/undo/{case['history_id']}"
                )
            payload = response.get_json()
            assert response.status_code == 200, payload
            assert payload['success'] is True
            assert case['source'].exists()
            assert not exact_destination.exists()

            conn = database.get_db()
            book = conn.execute('SELECT * FROM books WHERE id = ?',
                                (case['book_id'],)).fetchone()
            history = conn.execute('SELECT * FROM history WHERE id = ?',
                                   (case['history_id'],)).fetchone()
            conn.close()
            assert book['path'] == str(case['source'])
            assert book['status'] == 'protected'
            assert history['status'] == 'undone'
    print('[PASS] verified folder and loose-file applies both undo to the exact source')


def test_startup_recovers_interrupted_verified_move():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir))
        source_inventory = build_file_inventory(case['source'], [case['library_root']])
        source_digest = inventory_digest(source_inventory)
        conn = database.get_db()
        conn.execute("UPDATE history SET status = 'applying' WHERE id = ?", (case['history_id'],))
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO file_operations
                          (history_id, book_id, operation_type, source_path,
                           destination_path, database_destination, source_digest,
                           status, destination_existed)
                          VALUES (?, ?, 'apply_fix', ?, ?, ?, ?, 'moving', 0)''',
                       (case['history_id'], case['book_id'], str(case['source']),
                        str(case['destination']), str(case['destination']), source_digest))
        operation_id = cursor.lastrowid
        store_inventory(cursor, operation_id, 'source', source_inventory)
        conn.commit()
        conn.close()

        case['destination'].parent.mkdir(parents=True)
        shutil.move(str(case['source']), str(case['destination']))
        result = recover_interrupted_apply_fixes(database.get_db, test_config(case['library_root']))
        assert result == {'recovered': 1, 'manual_recovery': 0}
        assert case['source'].exists()
        assert not case['destination'].exists()
        assert build_file_inventory(case['source'], [case['library_root']]) == source_inventory

        conn = database.get_db()
        history = conn.execute('SELECT * FROM history WHERE id = ?', (case['history_id'],)).fetchone()
        operation = conn.execute('SELECT * FROM file_operations WHERE id = ?', (operation_id,)).fetchone()
        rollback_rows = conn.execute(
            "SELECT * FROM file_operation_inventory WHERE operation_id = ? AND phase = 'rollback'",
            (operation_id,),
        ).fetchall()
        conn.close()
        assert history['status'] == 'pending_fix'
        assert operation['status'] == 'rolled_back'
        assert operation['source_digest'] == operation['rollback_digest']
        assert rollback_rows and all(row['verified'] == 1 for row in rollback_rows)
    print('[PASS] startup recovery uses the persisted receipt and verifies restoration')


def test_startup_recovers_crash_before_empty_destination_move():
    with tempfile.TemporaryDirectory() as temp_dir:
        case = create_case(Path(temp_dir), destination_exists=True)
        source_inventory = build_file_inventory(case['source'], [case['library_root']])
        source_digest = inventory_digest(source_inventory)
        conn = database.get_db()
        conn.execute("UPDATE history SET status = 'applying', move_destination = ?, "
                     "move_destination_existed = 1 WHERE id = ?",
                     (str(case['destination']), case['history_id']))
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO file_operations
                          (history_id, book_id, operation_type, source_path,
                           destination_path, database_destination, source_digest,
                           status, destination_existed)
                          VALUES (?, ?, 'apply_fix', ?, ?, ?, ?, 'moving', 1)''',
                       (case['history_id'], case['book_id'], str(case['source']),
                        str(case['destination']), str(case['destination']), source_digest))
        operation_id = cursor.lastrowid
        store_inventory(cursor, operation_id, 'source', source_inventory)
        conn.commit()
        conn.close()

        result = recover_interrupted_apply_fixes(database.get_db, test_config(case['library_root']))
        assert result == {'recovered': 1, 'manual_recovery': 0}
        assert build_file_inventory(case['source'], [case['library_root']]) == source_inventory
        assert case['destination'].is_dir()
        assert list(case['destination'].iterdir()) == []

        conn = database.get_db()
        history = conn.execute('SELECT * FROM history WHERE id = ?',
                               (case['history_id'],)).fetchone()
        operation = conn.execute('SELECT * FROM file_operations WHERE id = ?',
                                 (operation_id,)).fetchone()
        conn.close()
        assert history['status'] == 'pending_fix'
        assert operation['status'] == 'rolled_back'
        assert operation['source_digest'] == operation['rollback_digest']
    print('[PASS] startup recovery accepts an intact source plus its original empty destination')


def test_rollback_preserves_nested_configured_root():
    with tempfile.TemporaryDirectory() as temp_dir:
        library_root = Path(temp_dir) / 'library'
        output_root = library_root / 'organized'
        source = library_root / 'Old Author' / 'Old Title'
        destination = output_root / 'New Author' / 'New Title'
        destination.mkdir(parents=True)
        (destination / 'Part 01.mp3').write_bytes(b'nested-root rollback')

        success, detail = rollback_filesystem_move(
            source,
            destination,
            [library_root, output_root],
        )
        assert success, detail
        assert source.is_dir()
        assert (source / 'Part 01.mp3').read_bytes() == b'nested-root rollback'
        assert output_root.is_dir()
        assert list(output_root.iterdir()) == []
    print('[PASS] rollback cleanup never removes a nested configured root')


def main():
    original_db_path = database._db_path
    try:
        test_folder_db_failure_restores_verified_inventory()
        test_stale_unique_path_collision_is_blocked_before_move()
        test_case_variant_database_collision_is_blocked_before_move()
        test_configured_root_can_never_be_moved()
        test_single_file_db_failure_restores_exact_file()
        test_destination_mismatch_is_rejected_and_rolled_back()
        test_rollback_failure_requires_manual_recovery()
        test_success_commits_only_after_verified_handoff()
        test_successful_folder_and_file_moves_remain_undoable()
        test_startup_recovers_interrupted_verified_move()
        test_startup_recovers_crash_before_empty_destination_move()
        test_rollback_preserves_nested_configured_root()
    finally:
        database.set_db_path(original_db_path)
    print('\nAll apply-fix atomicity and transfer-receipt tests passed.')


if __name__ == '__main__':
    main()
