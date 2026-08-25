# Troubleshooting

## Path does not exist (Docker)

Use the container side of the volume mount in Settings:

```yaml
- /host/path:/audiobooks
```

Enter `/audiobooks`, not `/host/path`. The container cannot access unmounted host paths.

## Permission denied

The process needs read/write access to the library, destination, data directory, and database. For Docker, check host ownership or set matching `PUID`/`PGID`. Use targeted permissions for the intended directories; do not run broad recursive permission changes on a library or filesystem.

## Wrong match or series not detected

Leave uncertain suggestions pending or reject them. Adjust language/provider/safety settings, edit the book manually, or run a deep rescan. Enable series grouping and ensure the chosen template includes the series fields you need.

## Apply operation blocked

Common reasons are an existing non-empty destination, a destination already assigned to another book, a path outside configured roots, or a missing source. Correct the path/destination and retry; the app deliberately refuses ambiguous merges and overwrites.

## Interrupted apply or rollback

Beta.161 develop builds recover interrupted `applying` operations on startup. If the source inventory can be restored and verified, the item returns to pending; otherwise it is marked for manual recovery. Inspect History's receipt and do not delete either side until the paths and inventory records are understood.

## Undo

Use **History → Undo**. Undo is blocked if both sides exist or either path leaves a configured root. Resolve the ambiguity manually instead of overwriting data.

## Provider errors or rate limits

Check provider credentials, model ID, endpoint reachability, and Docker networking. The local request limiter defaults to 200/hour and clamps to 10–500/hour; wait for provider backoff/circuit-breaker recovery before repeatedly retrying.

## Database locked or worker stuck

Stop overlapping scans/process requests and allow the current worker to finish. Restart if the lock persists, then inspect the first error in the logs. Keep one active worker per data directory.

## Watch-folder items do not move

Check watch mode, source/output paths, file age delay, and permissions. Hard links require source and destination to share a filesystem; if they do not, the app fails without deleting the source.

## Pre-sort plan is pending or cannot apply

Open **Pre-sort** and expand the inventory. Medium-confidence plans require manual Apply. Any unassigned companion file, symlink, destination collision, unsupported nested content, or changed source blocks the operation. Multipart sibling folders are deferred as a group until every part satisfies the pre-sort settle time.

## Pre-sort Undo is unavailable

Undo requires every recorded destination file to remain unchanged and every original source path to remain absent. It refuses to overwrite or guess if both sides exist, neither side exists, hashes changed, or the normal watch pipeline already consumed the output. A successful Undo returns the plan to Pending; Apply it again or select **Treat as One Book** to release the restored source to normal ingestion. Inspect the retained receipt before manually changing either side.

## Pre-sort reports manual recovery

Do not delete either path. Compare the receipt's exact source/destination mapping, sizes, and SHA-256 hashes with the filesystem. Manual recovery means startup or rollback found an ambiguous state and intentionally stopped rather than risking an overwrite.

## Logs and bug reports

```bash
docker compose logs -f library-manager
```

Settings can generate a bug report containing version, sanitized configuration, recent errors, and system information. Do not post API keys, secrets, database files, or complete private library paths publicly.
