"""Recovery helpers for filesystem moves coordinated with database updates."""

import hashlib
import json
import logging
import os
import shutil
import stat
from pathlib import Path


logger = logging.getLogger(__name__)


class InventoryError(RuntimeError):
    """Raised when a complete, safe inventory cannot be produced."""


def _resolved_roots(paths):
    """Return unique, resolved roots for configured library locations."""
    roots = []
    for value in paths:
        if not value:
            continue
        root = Path(value).resolve()
        if root not in roots:
            roots.append(root)
    return roots


def _is_within_roots(path, roots):
    """Return whether path is contained by one of the configured roots."""
    resolved = Path(path).resolve()
    for root in roots:
        try:
            resolved.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _sha256_file(path):
    """Hash one stable regular file without following a last-second symlink."""
    digest = hashlib.sha256()
    flags = os.O_RDONLY
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW

    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise InventoryError(f"Could not safely open inventory file {path}: {exc}") from exc

    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise InventoryError(f"Inventory entry changed type while hashing: {path}")
        with os.fdopen(descriptor, 'rb', closefd=False) as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)

    stable_fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns')
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise InventoryError(f"Inventory file changed while hashing: {path}")
    return before.st_size, digest.hexdigest()


def build_file_inventory(root_path, allowed_roots):
    """Hash every entry below root and return a deterministic inventory."""
    root = Path(root_path)
    roots = _resolved_roots(allowed_roots)
    if not roots or not _is_within_roots(root, roots):
        raise InventoryError(f"Inventory path is outside configured roots: {root}")
    if not root.exists() and not root.is_symlink():
        raise InventoryError(f"Inventory path does not exist: {root}")

    paths = [root]
    if root.is_dir() and not root.is_symlink():
        for current, directory_names, file_names in os.walk(root, followlinks=False):
            current_path = Path(current)
            directory_names.sort()
            file_names.sort()
            paths.extend(current_path / name for name in directory_names)
            paths.extend(current_path / name for name in file_names)

    inventory = []
    seen = set()
    for path in paths:
        relative_path = '.' if path == root else path.relative_to(root).as_posix()
        if relative_path in seen:
            continue
        seen.add(relative_path)

        if path.is_symlink():
            target = os.readlink(path)
            target_bytes = os.fsencode(target)
            entry = {
                'relative_path': relative_path,
                'entry_type': 'symlink',
                'size': len(target_bytes),
                'sha256': hashlib.sha256(target_bytes).hexdigest(),
            }
        elif path.is_dir():
            entry = {
                'relative_path': relative_path,
                'entry_type': 'directory',
                'size': 0,
                'sha256': None,
            }
        elif path.is_file():
            if not _is_within_roots(path, roots):
                raise InventoryError(f"Inventory file resolves outside configured roots: {path}")
            size, sha256 = _sha256_file(path)
            entry = {
                'relative_path': relative_path,
                'entry_type': 'file',
                'size': size,
                'sha256': sha256,
            }
        else:
            raise InventoryError(f"Unsupported filesystem entry in inventory: {path}")
        inventory.append(entry)

    inventory.sort(key=lambda item: item['relative_path'])
    return inventory


def inventory_digest(inventory):
    """Return one digest covering paths, types, sizes, and content hashes."""
    encoded = json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def inventories_match(expected, observed):
    """Compare complete inventories, including directory layout."""
    return inventory_digest(expected) == inventory_digest(observed) and expected == observed


def store_inventory(cursor, operation_id, phase, inventory, expected=None):
    """Persist an observed inventory and whether each entry matched source."""
    expected_by_path = {
        item['relative_path']: item for item in (expected or inventory)
    }
    cursor.execute('DELETE FROM file_operation_inventory WHERE operation_id = ? AND phase = ?',
                   (operation_id, phase))
    for item in inventory:
        verified = int(expected_by_path.get(item['relative_path']) == item)
        cursor.execute('''INSERT INTO file_operation_inventory
                          (operation_id, phase, relative_path, entry_type, size, sha256, verified)
                          VALUES (?, ?, ?, ?, ?, ?, ?)''',
                       (operation_id, phase, item['relative_path'], item['entry_type'],
                        item['size'], item['sha256'], verified))


def load_inventory(cursor, operation_id, phase='source'):
    """Load a stored inventory in deterministic comparison form."""
    cursor.execute('''SELECT relative_path, entry_type, size, sha256
                      FROM file_operation_inventory
                      WHERE operation_id = ? AND phase = ?
                      ORDER BY relative_path''', (operation_id, phase))
    return [dict(row) for row in cursor.fetchall()]


def _remove_empty_parents(start_path, roots):
    """Remove empty destination parents without ever removing a configured root."""
    current = Path(start_path)
    while current.exists() and current.is_dir():
        current_resolved = current.resolve()
        if (not _is_within_roots(current, roots)
                or any(current_resolved == root for root in roots)):
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def rollback_filesystem_move(source_path, destination_path, allowed_roots,
                             recreate_empty_destination=False):
    """Restore a move without overwriting data on either side.

    Returns ``(success, detail)``. A successful result means the original source
    exists and the moved destination no longer contains the moved object.
    """
    source = Path(source_path)
    destination = Path(destination_path)
    roots = _resolved_roots(allowed_roots)

    if not roots:
        return False, "no configured roots are available for rollback"
    if not _is_within_roots(source, roots) or not _is_within_roots(destination, roots):
        return False, "rollback path is outside the configured library/watch roots"

    source_exists = source.exists() or source.is_symlink()
    destination_exists = destination.exists() or destination.is_symlink()

    if source_exists and destination_exists:
        if (recreate_empty_destination and destination.is_dir()
                and not any(destination.iterdir())):
            return True, "filesystem was already at the original source and empty destination"
        return False, "both source and destination exist; refusing to overwrite either path"
    if not source_exists and not destination_exists:
        return False, "neither source nor destination exists"

    if not source_exists:
        try:
            source.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(destination), str(source))
        except Exception as exc:
            return False, f"could not restore destination to source: {exc}"

    if recreate_empty_destination:
        try:
            destination.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            return False, f"source restored but empty destination could not be recreated: {exc}"
    else:
        _remove_empty_parents(destination.parent, roots)

    return True, "filesystem move rolled back"


def recover_interrupted_apply_fixes(get_db, config):
    """Recover rows left in ``applying`` by an interrupted process.

    The critical database transaction writes ``books.path`` and marks history
    fixed together. Therefore an ``applying`` row means that transaction never
    committed and any completed filesystem move must be compensated.
    """
    configured_roots = list(config.get('library_paths', []))
    configured_roots.extend([
        config.get('watch_folder', ''),
        config.get('watch_output_folder', ''),
    ])
    roots = _resolved_roots(configured_roots)

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('''SELECT h.id, h.old_path, h.new_path, h.move_destination,
                                 h.move_destination_existed,
                                 fo.id AS operation_id,
                                 fo.source_path AS operation_source,
                                 fo.destination_path AS operation_destination,
                                 fo.destination_existed AS operation_destination_existed
                          FROM history h
                          LEFT JOIN file_operations fo ON fo.id = (
                              SELECT MAX(candidate.id) FROM file_operations candidate
                              WHERE candidate.history_id = h.id
                          )
                          WHERE h.status = 'applying' ''')
        rows = cursor.fetchall()
    except Exception:
        conn.close()
        raise

    recovered = 0
    manual = 0

    def mark_manual_recovery(history_id, operation_id, message):
        if operation_id is not None:
            cursor.execute('''UPDATE file_operations
                              SET status = 'rollback_failed', error_message = ?,
                                  updated_at = CURRENT_TIMESTAMP
                              WHERE id = ?''', (message, operation_id))
        cursor.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                       ('error', message, history_id))

    for row in rows:
        history_id = row['id']
        operation_id = row['operation_id']
        source_value = row['operation_source'] or row['old_path']
        destination_value = (
            row['operation_destination']
            or row['move_destination']
            or row['new_path']
        )
        recreate_empty = bool(
            row['operation_destination_existed']
            if operation_id is not None
            else row['move_destination_existed']
        )

        if not source_value or not destination_value:
            message = "Interrupted apply has incomplete path data; manual recovery required"
            mark_manual_recovery(history_id, operation_id, message)
            manual += 1
            continue

        source = Path(source_value)
        destination = Path(destination_value)
        if not _is_within_roots(source, roots) or not _is_within_roots(destination, roots):
            message = "Interrupted apply path is outside configured roots; manual recovery required"
            mark_manual_recovery(history_id, operation_id, message)
            manual += 1
            continue

        if operation_id is None and not source.exists():
            message = "Interrupted apply has no inventory receipt; manual recovery required"
            mark_manual_recovery(history_id, operation_id, message)
            manual += 1
            continue

        success, detail = rollback_filesystem_move(
            source,
            destination,
            roots,
            recreate_empty_destination=recreate_empty,
        )
        if success:
            receipt_verified = True
            rollback_inventory = None
            rollback_digest = None
            if operation_id is not None:
                try:
                    source_inventory = load_inventory(cursor, operation_id, 'source')
                    rollback_inventory = build_file_inventory(source, roots)
                    rollback_digest = inventory_digest(rollback_inventory)
                    receipt_verified = inventories_match(source_inventory, rollback_inventory)
                    store_inventory(cursor, operation_id, 'rollback', rollback_inventory,
                                    expected=source_inventory)
                except Exception as exc:
                    receipt_verified = False
                    detail = f"{detail}; rollback inventory could not be verified: {exc}"

            if receipt_verified:
                if operation_id is not None:
                    cursor.execute('''UPDATE file_operations
                                      SET status = 'rolled_back', rollback_digest = ?,
                                          error_message = 'Interrupted operation recovered on startup',
                                          updated_at = CURRENT_TIMESTAMP
                                      WHERE id = ?''', (rollback_digest, operation_id))
                cursor.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                               ('pending_fix', 'Interrupted filesystem move was rolled back and verified; safe to retry', history_id))
                recovered += 1
            else:
                if operation_id is not None:
                    cursor.execute('''UPDATE file_operations
                                      SET status = 'rollback_failed', rollback_digest = ?,
                                          error_message = ?, updated_at = CURRENT_TIMESTAMP
                                      WHERE id = ?''', (rollback_digest, detail, operation_id))
                message = f"Interrupted apply inventory verification failed: {detail}; manual recovery required"
                cursor.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                               ('error', message, history_id))
                manual += 1
        else:
            message = f"Interrupted apply could not be recovered: {detail}; manual recovery required"
            if operation_id is not None:
                cursor.execute('''UPDATE file_operations
                                  SET status = 'rollback_failed', error_message = ?,
                                      updated_at = CURRENT_TIMESTAMP
                                  WHERE id = ?''', (detail, operation_id))
            cursor.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                           ('error', message, history_id))
            manual += 1

    conn.commit()
    conn.close()
    if recovered or manual:
        logger.warning("Recovered %s interrupted apply operation(s); %s require manual recovery",
                       recovered, manual)
    return {'recovered': recovered, 'manual_recovery': manual}
