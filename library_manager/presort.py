"""Safe, reviewable watch-folder pre-sort operations for issues #292/#298."""

import hashlib
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path

from library_manager.file_transactions import build_file_inventory
from library_manager.models.book_profile import detect_multibook_vs_chapters
from library_manager.utils import AUDIO_EXTENSIONS, sanitize_path_component


logger = logging.getLogger(__name__)

_MULTIPART_FOLDER_RE = re.compile(
    r'^(?P<base>.+?)[\s._-]+(?:cd|disc|disk|part|pt)[\s._-]*(?P<number>\d+)$',
    re.IGNORECASE,
)
_CHAPTER_FILE_RE = re.compile(
    r'(^|[\s._-])(?:chapter|ch|track|part|pt|cd|disc|disk|section)[\s._-]*\d*($|[\s._-])',
    re.IGNORECASE,
)
_EXPLICIT_BOOK_RE = re.compile(
    r'(?:^|[\s._-])(?:book|volume|vol\.?)\s*(\d+)(?=$|[\s._-])|(?:^|[\s._-])#(\d+)(?=$|[\s._-])',
    re.IGNORECASE,
)
class PresortError(RuntimeError):
    """Raised when a pre-sort plan cannot be completed safely."""


def _inside(path, root, allow_root=False):
    path_value = Path(path).resolve()
    root_value = Path(root).resolve()
    try:
        relative = path_value.relative_to(root_value)
    except ValueError:
        return False
    return allow_root or relative != Path('.')


def _exists(path):
    path_value = Path(path)
    return path_value.exists() or path_value.is_symlink()


def _relative(path, root):
    return Path(path).resolve().relative_to(Path(root).resolve()).as_posix()


def _direct_watch_child(path, watch_root):
    watch_root = Path(watch_root).resolve()
    relative = Path(path).resolve().relative_to(watch_root)
    if not relative.parts:
        raise PresortError(f'Pre-sort path cannot be the watch root: {path}')
    return watch_root / relative.parts[0]


def _case_variant_path(path, root):
    """Return an existing differently-cased component for a proposed path."""
    root = Path(root).resolve()
    proposed = Path(path)
    try:
        relative = proposed.relative_to(root)
    except ValueError:
        return None
    current = root
    for part in relative.parts:
        if not current.exists() or not current.is_dir():
            return None
        matches = [child for child in current.iterdir() if child.name.casefold() == part.casefold()]
        if not matches:
            return None
        exact = next((child for child in matches if child.name == part), None)
        if exact is None:
            return matches[0]
        current = exact
    return None


def _stable_file_receipt(path, root):
    inventory = build_file_inventory(path, [root])
    if len(inventory) != 1 or inventory[0]['entry_type'] != 'file':
        raise PresortError(f"Pre-sort only moves regular files: {path}")
    return inventory[0]


def _content_digest(items):
    content = [
        {
            'size': int(item['size']),
            'sha256': item['sha256'],
        }
        for item in items
        if item.get('entry_type') == 'file'
    ]
    content.sort(key=lambda item: (item['sha256'], item['size']))
    encoded = json.dumps(content, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _all_files(folder):
    """Return all regular files and unsupported entries without following links."""
    folder = Path(folder)
    files = []
    unsupported = []
    for current, directory_names, file_names in os.walk(folder, followlinks=False):
        current_path = Path(current)
        kept_directories = []
        for name in sorted(directory_names):
            child = current_path / name
            if child.is_symlink():
                unsupported.append(child)
            else:
                kept_directories.append(name)
        directory_names[:] = kept_directories
        for name in sorted(file_names):
            child = current_path / name
            if child.is_symlink() or not child.is_file():
                unsupported.append(child)
            else:
                files.append(child)
    return files, unsupported


def _is_settled(path, minimum_age, now):
    root = Path(path)
    entries = [root]
    if root.is_dir() and not root.is_symlink():
        for current, directory_names, file_names in os.walk(root, followlinks=False):
            current_path = Path(current)
            entries.extend(current_path / name for name in directory_names)
            entries.extend(current_path / name for name in file_names)
    try:
        newest = max(entry.lstat().st_mtime for entry in entries)
    except (FileNotFoundError, OSError):
        return False
    return (now - newest) >= minimum_age


def presort_path_is_settled(path, config, now=None):
    """Return whether a watch item satisfies the stricter pre-sort settle time."""
    current_time = time.time() if now is None else now
    minimum_age = max(
        int(config.get('watch_min_file_age_seconds', 30)),
        int(config.get('presort_settle_seconds', 300)),
    )
    return _is_settled(path, minimum_age, current_time)


def _unique_destination(candidate, used, existing_paths=None):
    candidate = Path(candidate)
    existing_paths = existing_paths or set()
    counter = 2
    current = candidate
    while (str(current).casefold() in used
           or str(current).casefold() in existing_paths):
        current = candidate.with_name(f"{candidate.stem} [{counter}]{candidate.suffix}")
        counter += 1
    used.add(str(current).casefold())
    return current


def _item(source, destination, root, note=''):
    receipt = _stable_file_receipt(source, root)
    return {
        'source_path': str(Path(source).resolve()),
        'destination_path': str(Path(destination).resolve()) if destination else None,
        'entry_type': receipt['entry_type'],
        'size': receipt['size'],
        'sha256': receipt['sha256'],
        'note': note,
    }


def _unassigned_item(source, root, note):
    item = _item(source, None, root, note)
    item['entry_type'] = 'unassigned'
    return item


def _unsupported_item(source, root, note):
    """Inventory a symlink/unsupported entry without making it applicable."""
    try:
        inventory = build_file_inventory(source, [root])
        entry = inventory[0] if len(inventory) == 1 else {}
    except Exception:
        entry = {}
    return {
        'source_path': str(Path(source)),
        'destination_path': None,
        'entry_type': 'unassigned',
        'size': entry.get('size', 0),
        'sha256': entry.get('sha256'),
        'note': note,
    }


def _identification_key(result):
    if not result or result.get('_no_match'):
        return None
    for key in ('book_id', 'asin', 'audible_id', 'id'):
        value = str(result.get(key) or '').strip()
        if value:
            return f'{key}:{value.casefold()}'
    author = str(result.get('author') or '').strip().casefold()
    title = str(result.get('title') or '').strip().casefold()
    if title:
        return f'metadata:{author}|{title}'
    return None


def _explicit_book_number(path):
    match = _EXPLICIT_BOOK_RE.search(Path(path).stem)
    if not match:
        return None
    return match.group(1) or match.group(2)


def _split_plan(folder, watch_root, identify_audio=None):
    folder = Path(folder)
    direct_files = sorted(
        path for path in folder.iterdir()
        if path.is_file() and not path.is_symlink()
    )
    audio_files = [path for path in direct_files if path.suffix.lower() in AUDIO_EXTENSIONS]
    if len(audio_files) < 2:
        return None

    detection = detect_multibook_vs_chapters(audio_files, {})
    explicit_numbers = {audio_file: _explicit_book_number(audio_file) for audio_file in audio_files}
    distinct_explicit = {number for number in explicit_numbers.values() if number}
    explicit_multibook = len(distinct_explicit) >= 2
    group_by_audio = {}
    identified_names = {}

    if explicit_multibook:
        group_by_audio = {
            audio_file: f'book:{number}' if number else None
            for audio_file, number in explicit_numbers.items()
        }
        if all(explicit_numbers.values()):
            confidence = 'high'
            reason = detection.get(
                'reason', f'Found explicit book numbers: {sorted(distinct_explicit)}'
            )
        else:
            confidence = 'medium'
            reason = (
                f'Found explicit book numbers {sorted(distinct_explicit)}, but some audio files '
                'have no per-book assignment'
            )
    else:
        distinct_stems = {re.sub(r'\s+', ' ', path.stem).strip().casefold() for path in audio_files}
        has_chapter_names = sum(bool(_CHAPTER_FILE_RE.search(path.stem)) for path in audio_files)
        if len(audio_files) < 3 or len(distinct_stems) != len(audio_files) or has_chapter_names:
            return None
        confidence = 'medium'
        reason = (
            f"{len(audio_files)} distinctly named audio files may be separate books; "
            "review the proposed split before applying"
        )
        group_by_audio = {audio_file: f'file:{audio_file.stem.casefold()}' for audio_file in audio_files}

    nested_entries = [path for path in folder.iterdir() if path.is_dir() or path.is_symlink()]
    if confidence != 'high' and identify_audio:
        identified_keys = []
        fingerprint_groups = {}
        for audio_file in audio_files:
            try:
                result = identify_audio(str(audio_file))
            except Exception as exc:
                logger.warning(f'Pre-sort fingerprint identification failed for {audio_file}: {exc}')
                result = None
            key = _identification_key(result)
            identified_keys.append(key)
            fingerprint_groups[audio_file] = key
            if key and result and result.get('title'):
                author = str(result.get('author') or '').strip()
                title = str(result['title']).strip()
                identified_names.setdefault(key, (
                    f'{author} - {title}' if author else title
                ))
        if all(identified_keys):
            distinct_identifications = set(identified_keys)
            if len(distinct_identifications) == 1:
                return None
            group_by_audio = fingerprint_groups
            confidence = 'high'
            reason = (
                f'Skaldleita fingerprints identified {len(distinct_identifications)} '
                f'distinct books across {len(audio_files)} files'
            )
        elif any(identified_keys):
            reason += '; only some files had fingerprint matches, so review is still required'

    grouped_audio = {}
    for audio_file in audio_files:
        group = group_by_audio.get(audio_file)
        if group:
            grouped_audio.setdefault(group, []).append(audio_file)

    target_by_group = {}
    used_folder_names = set()
    for group, group_files in grouped_audio.items():
        if group in identified_names:
            proposed_name = identified_names[group]
        elif len(group_files) == 1:
            proposed_name = group_files[0].stem
        elif group.startswith('book:'):
            proposed_name = f"{folder.name} - Book {group.split(':', 1)[1]}"
        else:
            proposed_name = group_files[0].stem
        safe_name = sanitize_path_component(proposed_name).strip() or 'Untitled Book'
        candidate = safe_name
        counter = 2
        while candidate.casefold() in used_folder_names:
            candidate = f"{safe_name} [{counter}]"
            counter += 1
        used_folder_names.add(candidate.casefold())
        target_by_group[group] = folder.parent / candidate

    target_by_stem = {
        audio_file.stem.casefold(): target_by_group.get(group_by_audio.get(audio_file))
        for audio_file in audio_files
    }

    items = []
    used_destinations = set()
    for source in direct_files:
        target_root = target_by_stem.get(source.stem.casefold())
        if target_root is None:
            items.append(_unassigned_item(
                source,
                watch_root,
                'No matching audiobook stem; assign this companion file manually',
            ))
            continue
        destination = _unique_destination(target_root / source.name, used_destinations)
        items.append(_item(source, destination, watch_root, 'Move into its own book folder'))

    for nested in nested_entries:
        nested_files, nested_unsupported = (
            _all_files(nested)
            if nested.is_dir() and not nested.is_symlink()
            else ([], [nested])
        )
        if nested_files or nested_unsupported:
            for source in nested_files:
                items.append(_unassigned_item(
                    source,
                    watch_root,
                    'Nested content cannot be assigned to one split book automatically',
                ))
            for source in nested_unsupported:
                items.append(_unsupported_item(
                    source,
                    watch_root,
                    'Symlink cannot be assigned to one split book automatically',
                ))
        else:
            # Empty directories contain no data, but still make automatic cleanup ambiguous.
            reason += f"; empty or unsupported nested entry requires review: {nested.name}"

    applicable = all(item['destination_path'] for item in items) and bool(items)
    return {
        'action': 'split',
        'display_name': folder.name,
        'source_paths': [str(folder.resolve())],
        'confidence': confidence,
        'reason': reason,
        'applicable': applicable,
        'auto_applicable': applicable and confidence == 'high',
        'items': items,
    }


def _multipart_groups(watch_root, minimum_age, now):
    groups = {}
    deferred = 0
    for child in sorted(Path(watch_root).iterdir()):
        if not child.is_dir() or child.is_symlink():
            continue
        match = _MULTIPART_FOLDER_RE.match(child.name)
        if not match:
            continue
        settled = _is_settled(child, minimum_age, now)
        if not settled:
            deferred += 1
        key = match.group('base').strip().casefold()
        groups.setdefault(key, []).append((
            int(match.group('number')),
            child,
            match.group('base').strip(),
            settled,
        ))
    deferred_group_paths = set()
    ready_groups = {}
    for key, parts in groups.items():
        if len(parts) >= 2 and not all(item[3] for item in parts):
            deferred_group_paths.update(str(item[1].resolve()) for item in parts)
            continue
        ready_groups[key] = [(number, path, base) for number, path, base, _ in parts]
    return ready_groups, deferred, deferred_group_paths


def _merge_plan(parts, watch_root):
    if len(parts) < 2:
        return None
    ordered = sorted(parts, key=lambda item: (item[0], item[1].name.casefold()))
    numbers = [item[0] for item in ordered]
    if len(set(numbers)) != len(numbers):
        return None

    base_name = sanitize_path_component(ordered[0][2]).strip() or 'Merged Book'
    destination_root = Path(watch_root) / base_name
    items = []
    used_destinations = set()
    unsupported_names = []
    for number, source_root, _ in ordered:
        files, unsupported = _all_files(source_root)
        unsupported_names.extend(str(path) for path in unsupported)
        for source in files:
            relative = source.relative_to(source_root)
            flattened_name = ' - '.join(relative.parts)
            proposed_name = f"{number:02d} - {flattened_name}"
            destination = _unique_destination(
                destination_root / proposed_name,
                used_destinations,
            )
            items.append(_item(
                source,
                destination,
                watch_root,
                f"Merge from part {number}",
            ))

    applicable = bool(items) and not unsupported_names and not _exists(destination_root)
    reason = f"Found multipart folders {', '.join(str(number) for number in numbers)} for '{base_name}'"
    if _exists(destination_root):
        reason += '; unsuffixed destination already exists and will not be merged automatically'
    if unsupported_names:
        reason += '; symlinks or unsupported entries require manual review'
    for unsupported in unsupported_names:
        items.append(_unsupported_item(
            unsupported,
            watch_root,
            'Unsupported entry cannot be moved automatically',
        ))
    return {
        'action': 'merge',
        'display_name': base_name,
        'source_paths': [str(item[1].resolve()) for item in ordered],
        'confidence': 'high',
        'reason': reason,
        'applicable': applicable,
        'auto_applicable': applicable,
        'items': items,
    }


def _flatten_plan(folder, watch_root):
    folder = Path(folder)
    files, unsupported = _all_files(folder)
    nested_files = [path for path in files if path.parent != folder]
    nested_audio = [path for path in nested_files if path.suffix.lower() in AUDIO_EXTENSIONS]
    if not nested_audio:
        return None

    top_groups = {path.relative_to(folder).parts[0].casefold() for path in nested_audio}
    confidence = 'high' if len(top_groups) == 1 else 'medium'
    reason = (
        f"Found {len(nested_files)} files below {len(top_groups)} nested folder"
        f"{'s' if len(top_groups) != 1 else ''}; flatten without overwriting names"
    )
    if len(top_groups) != 1:
        reason += '; multiple nested audio groups may be separate books and require manual handling'
    existing = {
        str(path).casefold() for path in folder.iterdir()
        if path.is_file() or path.is_symlink()
    }
    used_destinations = set()
    items = []
    for source in nested_files:
        relative = source.relative_to(folder)
        proposed_name = source.name
        direct_candidate = folder / proposed_name
        if (str(direct_candidate).casefold() in existing
                or str(direct_candidate).casefold() in used_destinations):
            proposed_name = ' - '.join(relative.parts)
        destination = _unique_destination(folder / proposed_name, used_destinations, existing)
        items.append(_item(source, destination, watch_root, 'Flatten nested file'))
    for source in unsupported:
        items.append(_unsupported_item(
            source,
            watch_root,
            'Symlink or unsupported entry requires manual review',
        ))

    # One redundant wrapper is safe to remove automatically. Multiple top-level
    # audio folders may represent separate books, so never combine them into one
    # watch-folder handoff even when their filenames do not collide.
    applicable = bool(items) and not unsupported and len(top_groups) == 1
    return {
        'action': 'flatten',
        'display_name': folder.name,
        'source_paths': [str(folder.resolve())],
        'confidence': confidence,
        'reason': reason,
        'applicable': applicable,
        'auto_applicable': applicable and confidence == 'high',
        'items': items,
    }


def _plan_key(plan):
    payload = {
        'action': plan['action'],
        'source_paths': sorted(plan['source_paths']),
        'items': [
            {
                'source_path': item['source_path'],
                'destination_path': item['destination_path'],
                'size': item['size'],
                'sha256': item['sha256'],
            }
            for item in plan['items']
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _persist_plan(cursor, watch_root, plan):
    key = _plan_key(plan)
    cursor.execute('SELECT id FROM presort_plans WHERE plan_key = ?', (key,))
    existing = cursor.fetchone()
    if existing:
        return existing['id'], False
    cursor.execute('''INSERT OR IGNORE INTO presort_plans
                      (plan_key, watch_root, action, display_name, source_paths,
                       confidence, reason, applicable, auto_applicable, status)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''',
                   (key, str(Path(watch_root).resolve()), plan['action'], plan['display_name'],
                    json.dumps(plan['source_paths']), plan['confidence'], plan['reason'],
                    int(plan['applicable']), int(plan['auto_applicable'])))
    if cursor.rowcount != 1:
        cursor.execute('SELECT id FROM presort_plans WHERE plan_key = ?', (key,))
        return cursor.fetchone()['id'], False
    plan_id = cursor.lastrowid
    for position, item in enumerate(plan['items']):
        cursor.execute('''INSERT INTO presort_plan_items
                          (plan_id, position, source_path, destination_path, entry_type,
                           size, sha256, note)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (plan_id, position, item['source_path'], item['destination_path'],
                        item['entry_type'], item['size'], item['sha256'], item['note']))
    return plan_id, True


def scan_presort_plans(get_db, config, now=None, identify_audio=None):
    """Scan the watch root and persist non-destructive pre-sort plans."""
    if not config.get('presort_enabled', False):
        return {'enabled': False, 'created': 0, 'deferred': 0, 'plan_ids': []}
    watch_value = str(config.get('watch_folder', '')).strip()
    if not watch_value:
        raise PresortError('Pre-sort requires a configured watch folder')
    watch_root = Path(watch_value).resolve()
    if not watch_root.exists() or not watch_root.is_dir():
        raise PresortError(f'Watch folder does not exist: {watch_root}')

    current_time = time.time() if now is None else now
    minimum_age = max(
        int(config.get('watch_min_file_age_seconds', 30)),
        int(config.get('presort_settle_seconds', 300)),
    )
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT source_paths FROM presort_plans WHERE status IN ('pending', 'applying', 'error')")
    reserved = set()
    for row in cursor.fetchall():
        try:
            reserved.update(str(Path(value).resolve()) for value in json.loads(row['source_paths']))
        except (TypeError, ValueError):
            continue

    created_ids = []
    deferred = 0
    groups, group_deferred, deferred_group_paths = _multipart_groups(
        watch_root, minimum_age, current_time,
    )
    deferred += group_deferred
    reserved.update(deferred_group_paths)
    for parts in groups.values():
        sources = {str(item[1].resolve()) for item in parts}
        if sources & reserved:
            continue
        plan = _merge_plan(parts, watch_root)
        if not plan:
            continue
        plan_id, created = _persist_plan(cursor, watch_root, plan)
        reserved.update(sources)
        if created:
            created_ids.append(plan_id)

    for child in sorted(watch_root.iterdir()):
        child_value = str(child.resolve())
        if child_value in reserved or not child.is_dir() or child.is_symlink():
            continue
        if not _is_settled(child, minimum_age, current_time):
            deferred += 1
            continue
        identifier = identify_audio if config.get('presort_use_fingerprints', True) else None
        plan = _split_plan(child, watch_root, identify_audio=identifier)
        if plan is None:
            plan = _flatten_plan(child, watch_root)
        if plan is None:
            continue
        plan_id, created = _persist_plan(cursor, watch_root, plan)
        reserved.add(child_value)
        if created:
            created_ids.append(plan_id)

    conn.commit()
    conn.close()
    return {
        'enabled': True,
        'created': len(created_ids),
        'deferred': deferred,
        'deferred_paths': sorted(deferred_group_paths),
        'plan_ids': created_ids,
    }


def _load_plan(cursor, plan_id):
    cursor.execute('SELECT * FROM presort_plans WHERE id = ?', (plan_id,))
    plan = cursor.fetchone()
    if not plan:
        raise PresortError('Pre-sort plan not found')
    cursor.execute('''SELECT * FROM presort_plan_items
                      WHERE plan_id = ? ORDER BY position, id''', (plan_id,))
    return plan, cursor.fetchall()


def _validate_plan_item(item, watch_root):
    source = Path(item['source_path'])
    destination_value = item['destination_path']
    if not destination_value:
        raise PresortError(f"Plan contains an unassigned item: {source}")
    destination = Path(destination_value)
    if not _inside(source, watch_root) or not _inside(destination, watch_root):
        raise PresortError('Pre-sort path is outside the configured watch root')
    if source == destination:
        raise PresortError(f'Pre-sort source and destination are identical: {source}')
    if not source.exists() or source.is_symlink() or not source.is_file():
        raise PresortError(f'Pre-sort source is missing or unsupported: {source}')
    receipt = _stable_file_receipt(source, watch_root)
    if receipt['size'] != item['size'] or receipt['sha256'] != item['sha256']:
        raise PresortError(f'Pre-sort source changed after planning: {source}')
    if _exists(destination):
        raise PresortError(f'Pre-sort destination already exists: {destination}')
    case_variant = _case_variant_path(destination, watch_root)
    if case_variant:
        raise PresortError(f'Case-variant destination already exists: {case_variant}')


def _cleanup_empty_paths(paths, watch_root):
    watch_root = Path(watch_root).resolve()
    candidates = set()
    for value in paths:
        path = Path(value)
        if path.exists() and path.is_dir() and _inside(path, watch_root):
            for current, _, _ in os.walk(path, topdown=False):
                current_path = Path(current)
                if not _inside(current_path, watch_root):
                    continue
                try:
                    current_path.rmdir()
                except OSError:
                    continue
        candidates.add(path if path.exists() and path.is_dir() else path.parent)
    expanded = set()
    for candidate in candidates:
        current = candidate
        while _inside(current, watch_root):
            expanded.add(current)
            current = current.parent
    for candidate in sorted(expanded, key=lambda path: len(path.parts), reverse=True):
        try:
            candidate.rmdir()
        except OSError:
            continue


def _rollback_operation(cursor, operation_id, watch_root):
    cursor.execute('''SELECT * FROM presort_operation_items
                      WHERE operation_id = ? ORDER BY position DESC, id DESC''',
                   (operation_id,))
    items = cursor.fetchall()
    errors = []

    # Validate the complete rollback state before moving any file. This prevents
    # an ambiguous item late in the manifest from leaving an otherwise safe
    # operation only partially restored.
    source_ready = set()
    for item in items:
        source = Path(item['source_path'])
        destination = Path(item['destination_path'])
        if not _inside(source, watch_root) or not _inside(destination, watch_root):
            errors.append(f'path outside watch root: {source} -> {destination}')
            continue
        source_exists = _exists(source)
        destination_exists = _exists(destination)
        if source_exists and destination_exists:
            errors.append(f'both source and destination exist: {source} | {destination}')
            continue
        if not source_exists and not destination_exists:
            errors.append(f'both source and destination are missing: {source} | {destination}')
            continue
        current = source if source_exists else destination
        if current.is_symlink() or not current.is_file():
            errors.append(f'rollback item is missing or unsupported: {current}')
            continue
        try:
            receipt = _stable_file_receipt(current, watch_root)
        except Exception as exc:
            errors.append(f'could not verify rollback item {current}: {exc}')
            continue
        if receipt['size'] != item['size'] or receipt['sha256'] != item['sha256']:
            errors.append(f'rollback item hash mismatch: {current}')
            continue
        if source_exists:
            source_ready.add(item['id'])

    if errors:
        for item in items:
            cursor.execute('''UPDATE presort_operation_items
                              SET rollback_verified = ? WHERE id = ?''',
                           (int(item['id'] in source_ready), item['id']))
        verified_items = [dict(item) for item in items if item['id'] in source_ready]
        return False, '; '.join(dict.fromkeys(errors)), _content_digest(verified_items)

    for item in items:
        source = Path(item['source_path'])
        destination = Path(item['destination_path'])
        source_exists = _exists(source)
        destination_exists = _exists(destination)
        if source_exists and destination_exists:
            errors.append(f'both source and destination exist: {source} | {destination}')
            continue
        if not source_exists and not destination_exists:
            errors.append(f'both source and destination are missing: {source} | {destination}')
            continue
        if destination_exists:
            try:
                source.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination), str(source))
            except Exception as exc:
                errors.append(f'could not restore {destination} to {source}: {exc}')

    verified_items = []
    for item in items:
        source = Path(item['source_path'])
        verified = False
        try:
            receipt = _stable_file_receipt(source, watch_root)
            verified = receipt['size'] == item['size'] and receipt['sha256'] == item['sha256']
        except Exception as exc:
            errors.append(f'could not verify restored source {source}: {exc}')
        if not verified and not any(str(source) in error for error in errors):
            errors.append(f'restored source hash mismatch: {source}')
        cursor.execute('''UPDATE presort_operation_items
                          SET rollback_verified = ? WHERE id = ?''',
                       (int(verified), item['id']))
        verified_items.append(dict(item))

    _cleanup_empty_paths([item['destination_path'] for item in items], watch_root)
    return not errors, '; '.join(dict.fromkeys(errors)), _content_digest(verified_items)


def apply_presort_plan(get_db, config, plan_id, move_func=None):
    """Apply one plan with a durable per-file receipt and verified rollback."""
    if not config.get('presort_enabled', False):
        return False, 'Verified Pre-sort is disabled in Settings', None
    move = move_func or shutil.move
    conn = get_db()
    cursor = conn.cursor()
    try:
        plan, items = _load_plan(cursor, plan_id)
        if plan['status'] != 'pending':
            raise PresortError(f"Pre-sort plan is {plan['status']}, not pending")
        if not plan['applicable']:
            raise PresortError('Pre-sort plan contains unassigned or unsafe items and requires manual correction')
        watch_root = Path(plan['watch_root']).resolve()
        configured_watch = Path(str(config.get('watch_folder', '')).strip()).resolve()
        if watch_root != configured_watch:
            raise PresortError('Configured watch folder changed after this plan was created')

        destination_keys = set()
        for item in items:
            _validate_plan_item(item, watch_root)
            key = str(Path(item['destination_path']).resolve()).casefold()
            if key in destination_keys:
                raise PresortError(f"Duplicate pre-sort destination: {item['destination_path']}")
            destination_keys.add(key)

        plan_roots = {
            _direct_watch_child(item['source_path'], watch_root)
            for item in items
        }
        destination_roots = {
            _direct_watch_child(item['destination_path'], watch_root)
            for item in items
        }
        plan_roots.update(destination_roots)
        if plan['action'] != 'flatten':
            for destination_root in destination_roots:
                if _exists(destination_root):
                    raise PresortError(
                        f'Pre-sort destination folder already exists: {destination_root}'
                    )
                case_variant = _case_variant_path(destination_root, watch_root)
                if case_variant:
                    raise PresortError(
                        f'Case-variant destination folder already exists: {case_variant}'
                    )

        item_dicts = [dict(item) for item in items]
        source_digest = _content_digest(item_dicts)

        # Claim the plan under a SQLite write lock. Validation intentionally
        # happens before this short transaction so hashing large files does not
        # block unrelated database work.
        cursor.execute('BEGIN IMMEDIATE')
        cursor.execute('''UPDATE presort_plans
                          SET status = 'applying', error_message = NULL,
                              updated_at = CURRENT_TIMESTAMP
                          WHERE id = ? AND status = 'pending' ''', (plan_id,))
        if cursor.rowcount != 1:
            cursor.execute('SELECT status FROM presort_plans WHERE id = ?', (plan_id,))
            current = cursor.fetchone()
            status = current['status'] if current else 'missing'
            raise PresortError(f'Pre-sort plan is {status}, not pending')
        cursor.execute('''SELECT operation.watch_root, item.source_path, item.destination_path
                          FROM presort_operations operation
                          JOIN presort_operation_items item ON item.operation_id = operation.id
                          WHERE operation.status IN ('applying', 'undoing', 'rollback_failed')''')
        active_roots = set()
        for active in cursor.fetchall():
            active_watch = Path(active['watch_root']).resolve()
            for key in ('source_path', 'destination_path'):
                try:
                    active_roots.add(_direct_watch_child(active[key], active_watch))
                except (PresortError, ValueError):
                    continue
        conflicts = plan_roots & active_roots
        if conflicts:
            conflict_list = ', '.join(str(path) for path in sorted(conflicts))
            raise PresortError(f'Pre-sort path is reserved by another operation: {conflict_list}')
        cursor.execute('''INSERT INTO presort_operations
                          (plan_id, operation_type, watch_root, status, source_digest)
                          VALUES (?, ?, ?, 'applying', ?)''',
                       (plan_id, f"presort_{plan['action']}", str(watch_root), source_digest))
        operation_id = cursor.lastrowid
        for item in items:
            cursor.execute('''INSERT INTO presort_operation_items
                              (operation_id, position, source_path, destination_path,
                               entry_type, size, sha256, source_verified)
                              VALUES (?, ?, ?, ?, ?, ?, ?, 1)''',
                           (operation_id, item['position'], item['source_path'],
                            item['destination_path'], item['entry_type'], item['size'],
                            item['sha256']))
        conn.commit()

        try:
            for item in items:
                destination = Path(item['destination_path'])
                destination.parent.mkdir(parents=True, exist_ok=True)
                if _exists(destination):
                    raise PresortError(f'Pre-sort destination appeared during apply: {destination}')
                move(str(item['source_path']), str(destination))
                cursor.execute('''UPDATE presort_operation_items SET moved = 1
                                  WHERE operation_id = ? AND position = ?''',
                               (operation_id, item['position']))
                conn.commit()

            destination_verified = True
            for item in items:
                receipt = _stable_file_receipt(item['destination_path'], watch_root)
                verified = receipt['size'] == item['size'] and receipt['sha256'] == item['sha256']
                destination_verified = destination_verified and verified
                cursor.execute('''UPDATE presort_operation_items
                                  SET destination_verified = ?
                                  WHERE operation_id = ? AND position = ?''',
                               (int(verified), operation_id, item['position']))
            if not destination_verified:
                raise PresortError('Destination inventory does not match the pre-sort receipt')

            _cleanup_empty_paths(json.loads(plan['source_paths']), watch_root)
            cursor.execute('''UPDATE presort_operations
                              SET status = 'committed', destination_digest = ?,
                                  verified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                              WHERE id = ?''', (source_digest, operation_id))
            cursor.execute('''UPDATE presort_plans
                              SET status = 'applied', operation_id = ?, applied_at = CURRENT_TIMESTAMP,
                                  updated_at = CURRENT_TIMESTAMP
                              WHERE id = ?''', (operation_id, plan_id))
            conn.commit()
            return True, f"Applied {plan['action']} plan with {len(items)} verified file handoffs", operation_id
        except Exception as exc:
            rollback_ok, rollback_error, rollback_digest = _rollback_operation(
                cursor, operation_id, watch_root,
            )
            if rollback_ok:
                operation_status = 'rolled_back'
                plan_status = 'pending'
                detail = f'{exc}; filesystem rollback verified'
            else:
                operation_status = 'rollback_failed'
                plan_status = 'error'
                detail = f'{exc}; rollback failed: {rollback_error}; manual recovery required'
            cursor.execute('''UPDATE presort_operations
                              SET status = ?, rollback_digest = ?, error_message = ?,
                                  updated_at = CURRENT_TIMESTAMP WHERE id = ?''',
                           (operation_status, rollback_digest, detail, operation_id))
            cursor.execute('''UPDATE presort_plans
                              SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
                              WHERE id = ?''', (plan_status, detail, plan_id))
            conn.commit()
            return False, detail, operation_id
    except Exception as exc:
        conn.rollback()
        return False, str(exc), None
    finally:
        conn.close()


def undo_presort_operation(get_db, config, operation_id):
    """Undo a committed pre-sort operation when every destination is intact."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM presort_operations WHERE id = ?', (operation_id,))
        operation = cursor.fetchone()
        if not operation:
            raise PresortError('Pre-sort receipt not found')
        watch_root = Path(operation['watch_root']).resolve()
        configured_watch = Path(str(config.get('watch_folder', '')).strip()).resolve()
        if watch_root != configured_watch:
            raise PresortError('Configured watch folder changed; refusing to undo')

        cursor.execute('BEGIN IMMEDIATE')
        cursor.execute('''UPDATE presort_operations
                          SET status = 'undoing', error_message = NULL,
                              updated_at = CURRENT_TIMESTAMP
                          WHERE id = ? AND status = 'committed' ''', (operation_id,))
        if cursor.rowcount != 1:
            cursor.execute('SELECT status FROM presort_operations WHERE id = ?', (operation_id,))
            current = cursor.fetchone()
            status = current['status'] if current else 'missing'
            raise PresortError(f'Pre-sort receipt is {status}, not committed')
        conn.commit()

        success, detail, rollback_digest = _rollback_operation(cursor, operation_id, watch_root)
        if not success:
            cursor.execute('''UPDATE presort_operations
                              SET status = 'rollback_failed', error_message = ?, rollback_digest = ?,
                                  updated_at = CURRENT_TIMESTAMP WHERE id = ?''',
                           (detail, rollback_digest, operation_id))
            cursor.execute("UPDATE presort_plans SET status = 'error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (f'Undo failed: {detail}', operation['plan_id']))
            conn.commit()
            return False, f'Undo failed: {detail}'
        cursor.execute('''UPDATE presort_operations
                          SET status = 'undone', rollback_digest = ?, updated_at = CURRENT_TIMESTAMP
                          WHERE id = ?''', (rollback_digest, operation_id))
        cursor.execute("UPDATE presort_plans SET status = 'pending', error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                       (operation['plan_id'],))
        conn.commit()
        return True, 'Pre-sort operation undone and every source file verified; plan returned to review'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def reject_presort_plan(get_db, plan_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE presort_plans SET status = 'rejected', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                   (plan_id,))
    changed = cursor.rowcount
    conn.commit()
    conn.close()
    if not changed:
        return False, 'Only pending pre-sort plans can be rejected'
    return True, 'Pre-sort plan rejected; no files were changed'


def recover_interrupted_presort_operations(get_db, config):
    """Roll back operations interrupted during apply or Undo."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM presort_operations WHERE status IN ('applying', 'undoing') ORDER BY id")
    operations = cursor.fetchall()
    recovered = 0
    manual = 0
    configured_watch_value = str(config.get('watch_folder', '')).strip()
    configured_watch = Path(configured_watch_value).resolve() if configured_watch_value else None
    for operation in operations:
        watch_root = Path(operation['watch_root']).resolve()
        if configured_watch is None or configured_watch != watch_root:
            detail = 'Configured watch folder changed; manual recovery required'
            cursor.execute("UPDATE presort_operations SET status = 'rollback_failed', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (detail, operation['id']))
            cursor.execute("UPDATE presort_plans SET status = 'error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (detail, operation['plan_id']))
            manual += 1
            continue
        success, detail, rollback_digest = _rollback_operation(cursor, operation['id'], watch_root)
        if success and operation['status'] == 'undoing':
            cursor.execute("UPDATE presort_operations SET status = 'undone', rollback_digest = ?, error_message = 'Interrupted Undo recovered on startup', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (rollback_digest, operation['id']))
            cursor.execute("UPDATE presort_plans SET status = 'pending', error_message = 'Interrupted Undo completed and verified on startup; plan returned to review', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (operation['plan_id'],))
            recovered += 1
        elif success:
            cursor.execute("UPDATE presort_operations SET status = 'rolled_back', rollback_digest = ?, error_message = 'Interrupted operation recovered on startup', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (rollback_digest, operation['id']))
            cursor.execute("UPDATE presort_plans SET status = 'pending', error_message = 'Interrupted operation rolled back and verified; safe to retry', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (operation['plan_id'],))
            recovered += 1
        else:
            message = f'Interrupted pre-sort rollback failed: {detail}; manual recovery required'
            cursor.execute("UPDATE presort_operations SET status = 'rollback_failed', rollback_digest = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (rollback_digest, message, operation['id']))
            cursor.execute("UPDATE presort_plans SET status = 'error', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                           (message, operation['plan_id']))
            manual += 1
    conn.commit()
    conn.close()
    return {'recovered': recovered, 'manual_recovery': manual}


def active_presort_source_roots(get_db):
    """Return direct watch children reserved by plans or in-flight receipts."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT source_paths FROM presort_plans WHERE status IN ('pending', 'applying', 'error')")
    roots = []
    for row in cursor.fetchall():
        try:
            roots.extend(json.loads(row['source_paths']))
        except (TypeError, ValueError):
            continue

    # An Apply or Undo exposes destination/source paths one file at a time.
    # Reserve both sides' direct watch children so the ordinary watcher cannot
    # ingest an incomplete handoff while the receipt is still being reconciled.
    cursor.execute('''SELECT operation.watch_root, item.source_path, item.destination_path
                      FROM presort_operations operation
                      JOIN presort_operation_items item ON item.operation_id = operation.id
                      WHERE operation.status IN ('applying', 'undoing', 'rollback_failed')''')
    for row in cursor.fetchall():
        watch_root = Path(row['watch_root']).resolve()
        for key in ('source_path', 'destination_path'):
            candidate = Path(row[key]).resolve()
            try:
                relative = candidate.relative_to(watch_root)
            except ValueError:
                continue
            if relative.parts:
                roots.append(str(watch_root / relative.parts[0]))
    conn.close()
    return sorted({str(Path(value).resolve()) for value in roots})


def list_presort_plans(get_db, limit=200):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''SELECT p.*,
                             COALESCE(p.operation_id, (
                                 SELECT MAX(candidate.id) FROM presort_operations candidate
                                 WHERE candidate.plan_id = p.id
                             )) AS latest_operation_id,
                             (SELECT candidate.status FROM presort_operations candidate
                              WHERE candidate.plan_id = p.id ORDER BY candidate.id DESC LIMIT 1) AS operation_status
                      FROM presort_plans p
                      ORDER BY CASE p.status
                          WHEN 'pending' THEN 1 WHEN 'error' THEN 2 WHEN 'applied' THEN 3 ELSE 4 END,
                          p.created_at DESC, p.id DESC LIMIT ?''', (limit,))
    plans = []
    for row in cursor.fetchall():
        plan = dict(row)
        try:
            plan['source_paths_list'] = json.loads(plan['source_paths'])
        except (TypeError, ValueError):
            plan['source_paths_list'] = []
        cursor.execute('''SELECT * FROM presort_plan_items
                          WHERE plan_id = ? ORDER BY position, id''', (plan['id'],))
        plan['items'] = [dict(item) for item in cursor.fetchall()]
        plans.append(plan)
    conn.close()
    return plans


def get_presort_receipt(get_db, operation_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''SELECT operation.*, plan.action, plan.display_name, plan.reason
                      FROM presort_operations operation
                      JOIN presort_plans plan ON plan.id = operation.plan_id
                      WHERE operation.id = ?''', (operation_id,))
    operation = cursor.fetchone()
    if not operation:
        conn.close()
        return None
    cursor.execute('''SELECT * FROM presort_operation_items
                      WHERE operation_id = ? ORDER BY position, id''', (operation_id,))
    items = [dict(row) for row in cursor.fetchall()]
    result = {'operation': dict(operation), 'items': items}
    conn.close()
    return result


def apply_auto_presort_plans(get_db, config):
    """Apply all high-confidence plans when unattended mode is explicitly enabled."""
    if (not config.get('presort_enabled', False)
            or not config.get('presort_auto_apply', False)):
        return {'applied': 0, 'errors': 0, 'operation_ids': []}
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM presort_plans WHERE status = 'pending' AND auto_applicable = 1 ORDER BY id")
    plan_ids = [row['id'] for row in cursor.fetchall()]
    conn.close()
    applied = 0
    errors = 0
    operation_ids = []
    for plan_id in plan_ids:
        success, _, operation_id = apply_presort_plan(get_db, config, plan_id)
        if operation_id:
            operation_ids.append(operation_id)
        if success:
            applied += 1
        else:
            errors += 1
    return {'applied': applied, 'errors': errors, 'operation_ids': operation_ids}


__all__ = [
    'PresortError',
    'active_presort_source_roots',
    'apply_auto_presort_plans',
    'apply_presort_plan',
    'get_presort_receipt',
    'list_presort_plans',
    'presort_path_is_settled',
    'recover_interrupted_presort_operations',
    'reject_presort_plan',
    'scan_presort_plans',
    'undo_presort_operation',
]
