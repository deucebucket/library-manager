# How It Works

## User workflow

```text
Scan → identify candidates → review queue → apply selected fixes → verify in History
```

The scanner examines configured roots, skips known system/collection structures, validates eligible media, and creates or updates book records. Processing can be started from the Dashboard or run on the configured interval.

## Identification pipeline

The default modular order is:

1. Audio identification (Skaldleita/BookDB or configured fallbacks)
2. Audio credits and narrator information
3. Skaldleita requeue/verification when applicable
4. API metadata lookup
5. AI verification

Folder and filename hints remain a fallback for items that earlier layers cannot identify. Provider chains and pipeline order are configurable.

Metadata can come from Skaldleita/BookDB, Audnexus, OpenLibrary, Google Books, and Hardcover. AI choices are Gemini, OpenRouter, Ollama, and llama.cpp/OpenAI-compatible servers.

## Candidate checks

Results are compared with title, author, language, and series hints. Garbage title/author matches, placeholder authors, and third-party summary/derivative matches are rejected unless the source filename clearly indicates a summary. Uncertain or drastic changes are held for review according to safety settings.

## Naming and moves

After approval, the app builds a sanitized destination inside a configured library/watch root. It can move a complete book folder or a loose media file into a generated book folder. The database path, history, and queue are updated as one critical transaction; optional metadata embedding and post-processing hooks run afterward.

## Apply-fix transfer receipts (beta.161 develop)

The #300 safety flow:

1. Preflight rejects unsafe roots, existing file collisions, and known database path collisions.
2. A durable operation row and complete source inventory are stored before the filesystem changes.
3. Every relative path, entry type, size, and file SHA-256 hash is recorded; symlink targets are recorded without following them.
4. The destination inventory must exactly match the source inventory before the database transaction commits.
5. Verification or commit failures trigger a compensating move without overwriting either side, followed by source-inventory verification.
6. Startup recovery checks interrupted `applying` operations and either verifies rollback or marks manual recovery.

History displays the receipt status and opens a modal containing source/destination/rollback inventory entries, hashes, and verification flags.

![History transfer receipts](images/transfer-receipts-history.png)
![Committed transfer receipt](images/transfer-receipt-committed.png)
![Rollback transfer receipt](images/transfer-receipt-rollback.png)

This improves evidence of a handoff but cannot protect against hardware failure, permissions changed outside the app, or manual changes during a transfer. Keep independent backups.

## Undo and history

History retains original/new paths, status, errors, and optional metadata-embedding results. Undo validates both paths against configured roots and refuses ambiguous states rather than overwriting either side.
