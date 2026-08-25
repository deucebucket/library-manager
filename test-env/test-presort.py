#!/usr/bin/env python3
"""Backend regression coverage for verified pre-sort plans (#292/#298)."""

import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from library_manager import database
import library_manager.presort as presort_module
from library_manager.presort import (
    active_presort_source_roots,
    apply_auto_presort_plans,
    apply_presort_plan,
    get_presort_receipt,
    list_presort_plans,
    recover_interrupted_presort_operations,
    reject_presort_plan,
    scan_presort_plans,
    undo_presort_operation,
)


def make_case():
    temporary = tempfile.TemporaryDirectory(prefix='library-manager-presort-test-')
    root = Path(temporary.name)
    watch = root / 'watch'
    watch.mkdir()
    db_path = root / 'library.db'
    database.set_db_path(db_path)
    database.init_db()
    config = {
        'watch_folder': str(watch),
        'watch_min_file_age_seconds': 30,
        'presort_enabled': True,
        'presort_auto_apply': False,
        'presort_settle_seconds': 300,
    }
    return temporary, watch, config


def write_old(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    old = time.time() - 900
    os.utime(path, (old, old))
    current = path.parent
    while current != current.parent:
        os.utime(current, (old, old))
        if current.name == 'watch':
            break
        current = current.parent
    return path


def plans():
    return list_presort_plans(database.get_db)


def one_plan(action=None):
    found = plans()
    if action:
        found = [plan for plan in found if plan['action'] == action]
    assert len(found) == 1, [(plan['id'], plan['action'], plan['status']) for plan in found]
    return found[0]


def test_realistic_bundle_preview_apply_receipt_and_undo():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'BOOKTRACK - The Witcher'
        expected = {}
        for index, title in enumerate([
            'The Last Wish', 'Sword of Destiny', 'Blood of Elves',
            'Time of Contempt', 'Baptism of Fire', 'The Lady of the Lake',
        ]):
            payload = f'full audiobook {index}: {title}'.encode()
            write_old(bundle / f'{title}.mp3', payload)
            expected[f'{title}.mp3'] = payload

        summary = scan_presort_plans(database.get_db, config)
        assert summary['created'] == 1
        assert bundle.exists(), 'planning must never move files'
        plan = one_plan('split')
        assert plan['confidence'] == 'medium'
        assert plan['applicable'] == 1
        assert plan['auto_applicable'] == 0
        assert len(plan['items']) == 6
        assert str(bundle.resolve()) in active_presort_source_roots(database.get_db)

        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert success, message
        assert not bundle.exists()
        for filename, payload in expected.items():
            target = watch / Path(filename).stem / filename
            assert target.read_bytes() == payload

        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'committed'
        assert receipt['operation']['source_digest'] == receipt['operation']['destination_digest']
        assert len(receipt['items']) == 6
        assert all(item['source_verified'] and item['destination_verified'] for item in receipt['items'])

        success, message = undo_presort_operation(database.get_db, config, operation_id)
        assert success, message
        assert bundle.exists()
        assert {path.name: path.read_bytes() for path in bundle.iterdir()} == expected
        assert all(not (watch / Path(filename).stem).exists() for filename in expected)
        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'undone'
        assert all(item['rollback_verified'] for item in receipt['items'])
        assert one_plan('split')['status'] == 'pending'
        assert str(bundle.resolve()) in active_presort_source_roots(database.get_db)
        print('[PASS] realistic multi-book preview/apply/receipt/undo')
    finally:
        temporary.cleanup()


def test_explicit_books_are_high_confidence_but_chapters_are_not_planned():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Explicit Collection'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        chapter_book = watch / 'Normal Audiobook'
        write_old(chapter_book / 'Chapter 01.mp3', b'chapter one')
        write_old(chapter_book / 'Chapter 02.mp3', b'chapter two')
        write_old(chapter_book / 'Chapter 03.mp3', b'chapter three')

        scan_presort_plans(database.get_db, config)
        found = plans()
        assert len(found) == 1
        assert found[0]['display_name'] == bundle.name
        assert found[0]['confidence'] == 'high'
        assert found[0]['auto_applicable'] == 1
        print('[PASS] explicit multi-book detection avoids chapter false positives')
    finally:
        temporary.cleanup()


def test_explicit_multifile_books_share_one_destination_per_book():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Chronicles Collection'
        expected = {}
        for filename, payload in (
            ('Saga Book 1 - Chapter 01.mp3', b'book one chapter one'),
            ('Saga Book 1 - Chapter 02.mp3', b'book one chapter two'),
            ('Saga Book 2 - Chapter 01.mp3', b'book two chapter one'),
            ('Saga Book 2 - Chapter 02.mp3', b'book two chapter two'),
        ):
            write_old(bundle / filename, payload)
            expected[filename] = payload

        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        assert plan['confidence'] == 'high' and plan['auto_applicable'] == 1
        destination_parents = {
            Path(item['destination_path']).parent.name for item in plan['items']
        }
        assert destination_parents == {
            'Chronicles Collection - Book 1',
            'Chronicles Collection - Book 2',
        }
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert success, message
        for filename, payload in expected.items():
            number = '1' if 'Book 1' in filename else '2'
            assert (watch / f'Chronicles Collection - Book {number}' / filename).read_bytes() == payload
        success, message = undo_presort_operation(database.get_db, config, operation_id)
        assert success, message
        assert {path.name: path.read_bytes() for path in bundle.iterdir()} == expected
        print('[PASS] explicit chapter files group into one handoff folder per book')
    finally:
        temporary.cleanup()


def test_unassigned_companion_blocks_apply_without_losing_inventory():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Box Set'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        write_old(bundle / 'cover.jpg', b'shared cover')

        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        assert plan['applicable'] == 0
        unassigned = [item for item in plan['items'] if not item['destination_path']]
        assert len(unassigned) == 1
        assert unassigned[0]['source_path'].endswith('cover.jpg')
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert not success and operation_id is None
        assert 'unassigned' in message.lower()
        assert (bundle / 'cover.jpg').read_bytes() == b'shared cover'
        print('[PASS] shared companion is inventoried and blocks unsafe auto-assignment')
    finally:
        temporary.cleanup()


def test_multipart_folder_merge_receipt_and_undo():
    temporary, watch, config = make_case()
    try:
        part_one = watch / 'Long Story CD1'
        part_two = watch / 'Long Story CD2'
        write_old(part_one / 'track.mp3', b'part one')
        write_old(part_two / 'track.mp3', b'part two')
        write_old(part_two / 'art' / 'cover.jpg', b'part two cover')

        scan_presort_plans(database.get_db, config)
        plan = one_plan('merge')
        assert plan['confidence'] == 'high' and plan['auto_applicable'] == 1
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert success, message
        destination = watch / 'Long Story'
        assert (destination / '01 - track.mp3').read_bytes() == b'part one'
        assert (destination / '02 - track.mp3').read_bytes() == b'part two'
        assert (destination / '02 - art - cover.jpg').read_bytes() == b'part two cover'
        assert not part_one.exists() and not part_two.exists()

        success, message = undo_presort_operation(database.get_db, config, operation_id)
        assert success, message
        assert (part_one / 'track.mp3').read_bytes() == b'part one'
        assert (part_two / 'track.mp3').read_bytes() == b'part two'
        assert (part_two / 'art' / 'cover.jpg').read_bytes() == b'part two cover'
        assert not destination.exists()
        print('[PASS] multipart folder merge preserves order, hashes, and undo paths')
    finally:
        temporary.cleanup()


def test_nested_folder_flatten_and_undo():
    temporary, watch, config = make_case()
    try:
        book = watch / 'Narrator Layout'
        write_old(book / 'Narrator Name' / 'chapter.mp3', b'chapter')
        write_old(book / 'Narrator Name' / 'cover.jpg', b'cover')

        scan_presort_plans(database.get_db, config)
        plan = one_plan('flatten')
        assert plan['confidence'] == 'high'
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert success, message
        assert (book / 'chapter.mp3').read_bytes() == b'chapter'
        assert (book / 'cover.jpg').read_bytes() == b'cover'
        assert not (book / 'Narrator Name').exists()
        success, message = undo_presort_operation(database.get_db, config, operation_id)
        assert success, message
        assert (book / 'Narrator Name' / 'chapter.mp3').read_bytes() == b'chapter'
        print('[PASS] nested narrator layout flattens and restores exactly')
    finally:
        temporary.cleanup()


def test_multiple_nested_audio_groups_cannot_be_flattened_together():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Two Nested Books'
        write_old(bundle / 'Book Alpha' / 'chapter.mp3', b'alpha chapter')
        write_old(bundle / 'Book Beta' / 'chapter.mp3', b'beta chapter')

        scan_presort_plans(database.get_db, config)
        plan = one_plan('flatten')
        assert plan['confidence'] == 'medium'
        assert plan['applicable'] == 0
        assert plan['auto_applicable'] == 0
        assert 'may be separate books' in plan['reason']

        success, message, operation_id = apply_presort_plan(
            database.get_db, config, plan['id'],
        )
        assert not success and operation_id is None
        assert 'requires manual correction' in message.lower()
        assert (bundle / 'Book Alpha' / 'chapter.mp3').read_bytes() == b'alpha chapter'
        assert (bundle / 'Book Beta' / 'chapter.mp3').read_bytes() == b'beta chapter'
        assert not (bundle / 'chapter.mp3').exists()
        print('[PASS] separate nested audio groups cannot be flattened into one book')
    finally:
        temporary.cleanup()


def test_settle_window_and_source_mutation_block():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Still Downloading'
        (bundle).mkdir()
        (bundle / 'Book 1.mp3').write_bytes(b'one')
        (bundle / 'Book 2.mp3').write_bytes(b'two')
        summary = scan_presort_plans(database.get_db, config)
        assert summary['created'] == 0 and summary['deferred'] >= 1

        old = time.time() - 900
        for path in bundle.iterdir():
            os.utime(path, (old, old))
        os.utime(bundle, (old, old))
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        (bundle / 'Book 1.mp3').write_bytes(b'changed after planning')
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert not success and operation_id is None
        assert 'changed after planning' in message
        assert bundle.exists() and not (watch / 'Book 1').exists()
        print('[PASS] settle window and pre-apply rehash stop changing downloads')
    finally:
        temporary.cleanup()


def test_mid_move_failure_rolls_back_and_verifies_every_source():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Failure Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        calls = {'count': 0}

        def fail_second_move(source, destination):
            calls['count'] += 1
            if calls['count'] == 2:
                raise OSError('forced second move failure')
            return shutil.move(source, destination)

        success, message, operation_id = apply_presort_plan(
            database.get_db, config, plan['id'], move_func=fail_second_move,
        )
        assert not success and operation_id
        assert 'rollback verified' in message
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert (bundle / 'Book 2 - Beta.mp3').read_bytes() == b'beta'
        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'rolled_back'
        assert all(item['rollback_verified'] for item in receipt['items'])
        print('[PASS] mid-move failure restores and verifies complete source inventory')
    finally:
        temporary.cleanup()


def test_startup_recovers_interrupted_operation():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Interrupted Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        calls = {'count': 0}

        def interrupt_second_move(source, destination):
            calls['count'] += 1
            if calls['count'] == 2:
                raise KeyboardInterrupt('simulated process stop')
            return shutil.move(source, destination)

        try:
            apply_presort_plan(database.get_db, config, plan['id'], move_func=interrupt_second_move)
            raise AssertionError('expected simulated interruption')
        except KeyboardInterrupt:
            pass

        conn = database.get_db()
        operation = conn.execute("SELECT * FROM presort_operations WHERE status = 'applying'").fetchone()
        conn.close()
        assert operation
        result = recover_interrupted_presort_operations(database.get_db, config)
        assert result == {'recovered': 1, 'manual_recovery': 0}
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert (bundle / 'Book 2 - Beta.mp3').read_bytes() == b'beta'
        receipt = get_presort_receipt(database.get_db, operation['id'])
        assert receipt['operation']['status'] == 'rolled_back'
        assert all(item['rollback_verified'] for item in receipt['items'])
        print('[PASS] startup recovery reconciles a partially moved manifest')
    finally:
        temporary.cleanup()


def test_undo_ambiguity_preserves_both_sides_for_manual_recovery():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Ambiguous Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert success, message
        # Simulate an outside process recreating one original while destination remains.
        write_old(bundle / 'Book 1 - Alpha.mp3', b'outside data')
        success, message = undo_presort_operation(database.get_db, config, operation_id)
        assert not success and 'both source and destination exist' in message
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'outside data'
        assert (watch / 'Book 1 - Alpha' / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert not (bundle / 'Book 2 - Beta.mp3').exists()
        assert (watch / 'Book 2 - Beta' / 'Book 2 - Beta.mp3').read_bytes() == b'beta'
        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'rollback_failed'
        assert not any(item['rollback_verified'] for item in receipt['items'])
        print('[PASS] ambiguous undo preflights the full manifest without partial moves')
    finally:
        temporary.cleanup()


def test_concurrent_apply_and_undo_have_one_durable_owner():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Concurrent Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')

        apply_started = threading.Event()
        allow_apply = threading.Event()
        first_apply = []

        def blocking_apply_move(source, destination):
            if not apply_started.is_set():
                apply_started.set()
                assert allow_apply.wait(10), 'timed out waiting to release first apply'
            return shutil.move(source, destination)

        apply_thread = threading.Thread(
            target=lambda: first_apply.append(apply_presort_plan(
                database.get_db, config, plan['id'], move_func=blocking_apply_move,
            )),
        )
        apply_thread.start()
        assert apply_started.wait(10), 'first apply did not reach its filesystem handoff'
        reserved = set(active_presort_source_roots(database.get_db))
        assert str(bundle.resolve()) in reserved
        assert str((watch / 'Book 1 - Alpha').resolve()) in reserved
        assert str((watch / 'Book 2 - Beta').resolve()) in reserved
        second_apply = apply_presort_plan(database.get_db, config, plan['id'])
        assert not second_apply[0] and second_apply[2] is None
        assert 'not pending' in second_apply[1]
        allow_apply.set()
        apply_thread.join(10)
        assert not apply_thread.is_alive()
        assert first_apply and first_apply[0][0], first_apply
        operation_id = first_apply[0][2]

        conn = database.get_db()
        operation_count = conn.execute(
            'SELECT COUNT(*) FROM presort_operations WHERE plan_id = ?', (plan['id'],)
        ).fetchone()[0]
        conn.close()
        assert operation_count == 1

        undo_started = threading.Event()
        allow_undo = threading.Event()
        first_undo = []
        original_move = presort_module.shutil.move

        def blocking_undo_move(source, destination):
            if not undo_started.is_set():
                undo_started.set()
                assert allow_undo.wait(10), 'timed out waiting to release first Undo'
            return original_move(source, destination)

        presort_module.shutil.move = blocking_undo_move
        try:
            undo_thread = threading.Thread(
                target=lambda: first_undo.append(undo_presort_operation(
                    database.get_db, config, operation_id,
                )),
            )
            undo_thread.start()
            assert undo_started.wait(10), 'first Undo did not reach its filesystem handoff'
            reserved = set(active_presort_source_roots(database.get_db))
            assert str(bundle.resolve()) in reserved
            assert str((watch / 'Book 1 - Alpha').resolve()) in reserved
            assert str((watch / 'Book 2 - Beta').resolve()) in reserved
            second_undo = undo_presort_operation(database.get_db, config, operation_id)
            assert not second_undo[0] and 'not committed' in second_undo[1]
            allow_undo.set()
            undo_thread.join(10)
            assert not undo_thread.is_alive()
        finally:
            allow_undo.set()
            presort_module.shutil.move = original_move

        assert first_undo and first_undo[0][0], first_undo
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert (bundle / 'Book 2 - Beta.mp3').read_bytes() == b'beta'
        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'undone'
        assert one_plan('split')['status'] == 'pending'
        print('[PASS] concurrent Apply and Undo requests have one durable owner')
    finally:
        temporary.cleanup()


def test_startup_completes_interrupted_undo():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Interrupted Undo Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert success, message

        original_move = presort_module.shutil.move
        calls = {'count': 0}

        def interrupt_second_undo_move(source, destination):
            calls['count'] += 1
            if calls['count'] == 2:
                raise KeyboardInterrupt('simulated stop during Undo')
            return original_move(source, destination)

        presort_module.shutil.move = interrupt_second_undo_move
        try:
            try:
                undo_presort_operation(database.get_db, config, operation_id)
                raise AssertionError('expected simulated Undo interruption')
            except KeyboardInterrupt:
                pass
        finally:
            presort_module.shutil.move = original_move

        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'undoing'
        result = recover_interrupted_presort_operations(database.get_db, config)
        assert result == {'recovered': 1, 'manual_recovery': 0}
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert (bundle / 'Book 2 - Beta.mp3').read_bytes() == b'beta'
        receipt = get_presort_receipt(database.get_db, operation_id)
        assert receipt['operation']['status'] == 'undone'
        assert all(item['rollback_verified'] for item in receipt['items'])
        assert one_plan('split')['status'] == 'pending'
        print('[PASS] startup completes and verifies an interrupted Undo')
    finally:
        temporary.cleanup()


def test_reject_changes_only_plan_state():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Reject Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        disabled_config = dict(config, presort_enabled=False)
        applied, message, operation_id = apply_presort_plan(
            database.get_db, disabled_config, plan['id'],
        )
        assert not applied and operation_id is None and 'disabled' in message.lower()
        success, message = reject_presort_plan(database.get_db, plan['id'])
        assert success, message
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert not active_presort_source_roots(database.get_db)
        print('[PASS] reject is metadata-only and releases watch ingestion')
    finally:
        temporary.cleanup()


def test_auto_apply_only_touches_high_confidence_plans():
    temporary, watch, config = make_case()
    try:
        explicit = watch / 'Explicit Auto'
        write_old(explicit / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(explicit / 'Book 2 - Beta.mp3', b'beta')
        ambiguous = watch / 'Named Bundle'
        write_old(ambiguous / 'First Distinct Title.mp3', b'first')
        write_old(ambiguous / 'Second Distinct Title.mp3', b'second')
        write_old(ambiguous / 'Third Distinct Title.mp3', b'third')
        scan_presort_plans(database.get_db, config)
        config['presort_auto_apply'] = True
        result = apply_auto_presort_plans(database.get_db, config)
        assert result['applied'] == 1 and result['errors'] == 0
        by_name = {plan['display_name']: plan for plan in plans()}
        assert by_name['Explicit Auto']['status'] == 'applied'
        assert by_name['Named Bundle']['status'] == 'pending'
        assert ambiguous.exists()
        assert (watch / 'Book 1 - Alpha' / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        print('[PASS] unattended mode applies only high-confidence complete manifests')
    finally:
        temporary.cleanup()


def test_multipart_group_waits_for_every_part_to_settle():
    temporary, watch, config = make_case()
    try:
        first = watch / 'Settling Story CD1'
        second = watch / 'Settling Story CD2'
        write_old(first / 'track.mp3', b'old first')
        (second / 'track.mp3').parent.mkdir(parents=True)
        (second / 'track.mp3').write_bytes(b'new second')
        summary = scan_presort_plans(database.get_db, config)
        assert summary['created'] == 0
        assert set(summary['deferred_paths']) == {str(first.resolve()), str(second.resolve())}

        old = time.time() - 900
        os.utime(second / 'track.mp3', (old, old))
        os.utime(second, (old, old))
        summary = scan_presort_plans(database.get_db, config)
        assert summary['created'] == 1
        assert one_plan('merge')['display_name'] == 'Settling Story'
        print('[PASS] multipart group blocks every sibling until the whole group settles')
    finally:
        temporary.cleanup()


def test_watch_pipeline_reserves_a_pending_handoff_before_ingestion():
    temporary, watch, config = make_case()
    try:
        import app as app_module

        database.set_db_path(Path(temporary.name) / 'library.db')
        database.init_db()
        library_root = Path(temporary.name) / 'library'
        library_root.mkdir()
        config.update({
            'library_paths': [str(library_root)],
            'watch_output_folder': str(library_root),
            'watch_use_hard_links': False,
            'watch_delete_empty_folders': True,
        })
        bundle = watch / 'Reserved Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')

        processed = app_module.process_watch_folder(config)
        assert processed == 0
        plan = one_plan('split')
        assert plan['status'] == 'pending'
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        assert not any(library_root.iterdir())
        print('[PASS] watch pipeline reserves pending pre-sort sources before metadata ingestion')
    finally:
        temporary.cleanup()


def test_destination_collision_and_config_change_are_non_destructive():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Collision Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        collision = watch / 'Book 1 - Alpha' / 'Book 1 - Alpha.mp3'
        write_old(collision, b'pre-existing destination')
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert not success and operation_id is None
        assert 'destination already exists' in message.lower()
        assert collision.read_bytes() == b'pre-existing destination'
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'

        collision.unlink()
        collision.parent.rmdir()
        case_variant = watch / 'book 1 - alpha'
        case_variant.mkdir()
        success, message, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert not success and operation_id is None
        assert 'case-variant destination' in message.lower()
        assert (bundle / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha'
        case_variant.rmdir()

        changed_config = dict(config)
        changed_watch = Path(temporary.name) / 'different-watch'
        changed_watch.mkdir()
        changed_config['watch_folder'] = str(changed_watch)
        success, message, operation_id = apply_presort_plan(
            database.get_db, changed_config, plan['id'],
        )
        assert not success and operation_id is None
        assert 'watch folder changed' in message.lower()
        assert (bundle / 'Book 2 - Beta.mp3').read_bytes() == b'beta'
        print('[PASS] collisions and configuration drift preserve every path')
    finally:
        temporary.cleanup()


def test_symlink_is_inventoried_but_never_auto_moved():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Symlink Bundle'
        write_old(bundle / 'Book 1 - Alpha.mp3', b'alpha')
        write_old(bundle / 'Book 2 - Beta.mp3', b'beta')
        target = write_old(watch / 'outside.txt', b'outside')
        link = bundle / 'linked-cover.jpg'
        link.symlink_to(target)
        old = time.time() - 900
        os.utime(link, (old, old), follow_symlinks=False)
        os.utime(bundle, (old, old))
        scan_presort_plans(database.get_db, config)
        plan = one_plan('split')
        assert plan['applicable'] == 0
        assert any(item['source_path'].endswith('linked-cover.jpg') and not item['destination_path']
                   for item in plan['items'])
        success, _, operation_id = apply_presort_plan(database.get_db, config, plan['id'])
        assert not success and operation_id is None
        assert link.is_symlink() and target.read_bytes() == b'outside'
        print('[PASS] symlinks are visible in review and never followed or auto-moved')
    finally:
        temporary.cleanup()


def test_fingerprint_consensus_promotes_or_cancels_ambiguous_split():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Fingerprint Bundle'
        for title in ('Red Dawn', 'Blue Moon', 'Green Light'):
            write_old(bundle / f'{title}.mp3', title.encode())

        matches = {
            'Red Dawn.mp3': {'book_id': 'book-red', 'author': 'A. Author', 'title': 'Red Dawn'},
            'Blue Moon.mp3': {'book_id': 'book-blue', 'author': 'B. Author', 'title': 'Blue Moon'},
            'Green Light.mp3': {'book_id': 'book-green', 'author': 'C. Author', 'title': 'Green Light'},
        }
        summary = scan_presort_plans(
            database.get_db,
            config,
            identify_audio=lambda path: matches[Path(path).name],
        )
        assert summary['created'] == 1
        plan = one_plan('split')
        assert plan['confidence'] == 'high' and plan['auto_applicable'] == 1
        assert 'Skaldleita fingerprints identified 3 distinct books' in plan['reason']
        assert any('A. Author - Red Dawn' in item['destination_path'] for item in plan['items'])

        reject_presort_plan(database.get_db, plan['id'])
        same_book = watch / 'One Book Chapters'
        for title in ('Opening Scene', 'Middle Scene', 'Final Scene'):
            write_old(same_book / f'{title}.mp3', title.encode())
        summary = scan_presort_plans(
            database.get_db,
            config,
            identify_audio=lambda path: {
                'book_id': 'same-book', 'author': 'One Author', 'title': 'One Book'
            },
        )
        assert summary['created'] == 0
        assert not [plan for plan in plans() if plan['display_name'] == 'One Book Chapters']
        print('[PASS] fingerprint consensus promotes distinct books and cancels same-book false positives')
    finally:
        temporary.cleanup()


def test_fingerprint_assignment_groups_repeated_book_ids():
    temporary, watch, config = make_case()
    try:
        bundle = watch / 'Fingerprint Chapter Bundle'
        for title in ('Alpha Opening', 'Alpha Finale', 'Beta Complete'):
            write_old(bundle / f'{title}.mp3', title.encode())

        matches = {
            'Alpha Opening.mp3': {'book_id': 'book-alpha', 'author': 'A. Author', 'title': 'Alpha'},
            'Alpha Finale.mp3': {'book_id': 'book-alpha', 'author': 'A. Author', 'title': 'Alpha'},
            'Beta Complete.mp3': {'book_id': 'book-beta', 'author': 'B. Author', 'title': 'Beta'},
        }
        scan_presort_plans(
            database.get_db,
            config,
            identify_audio=lambda path: matches[Path(path).name],
        )
        plan = one_plan('split')
        assert plan['confidence'] == 'high' and plan['auto_applicable'] == 1
        destinations = {
            Path(item['source_path']).name: Path(item['destination_path']).parent.name
            for item in plan['items']
        }
        assert destinations == {
            'Alpha Opening.mp3': 'A. Author - Alpha',
            'Alpha Finale.mp3': 'A. Author - Alpha',
            'Beta Complete.mp3': 'B. Author - Beta',
        }
        assert '2 distinct books across 3 files' in plan['reason']
        print('[PASS] repeated fingerprint IDs group multiple files into the same book folder')
    finally:
        temporary.cleanup()


def main():
    test_realistic_bundle_preview_apply_receipt_and_undo()
    test_explicit_books_are_high_confidence_but_chapters_are_not_planned()
    test_explicit_multifile_books_share_one_destination_per_book()
    test_unassigned_companion_blocks_apply_without_losing_inventory()
    test_multipart_folder_merge_receipt_and_undo()
    test_nested_folder_flatten_and_undo()
    test_multiple_nested_audio_groups_cannot_be_flattened_together()
    test_settle_window_and_source_mutation_block()
    test_mid_move_failure_rolls_back_and_verifies_every_source()
    test_startup_recovers_interrupted_operation()
    test_undo_ambiguity_preserves_both_sides_for_manual_recovery()
    test_concurrent_apply_and_undo_have_one_durable_owner()
    test_startup_completes_interrupted_undo()
    test_reject_changes_only_plan_state()
    test_auto_apply_only_touches_high_confidence_plans()
    test_multipart_group_waits_for_every_part_to_settle()
    test_watch_pipeline_reserves_a_pending_handoff_before_ingestion()
    test_destination_collision_and_config_change_are_non_destructive()
    test_symlink_is_inventoried_but_never_auto_moved()
    test_fingerprint_consensus_promotes_or_cancels_ambiguous_split()
    test_fingerprint_assignment_groups_repeated_book_ids()
    print('\nAll verified pre-sort backend tests passed.')


if __name__ == '__main__':
    main()
