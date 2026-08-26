# FAQ

## General

### Is this free?

Yes. The app is free and open source. Hosted AI providers may require credentials and impose their own account/model limits. Check each provider for current terms.

Ollama and user-configured llama.cpp/OpenAI-compatible servers can run locally without an API key.

### Does it work with Audiobookshelf?

Yes! Enable **Series Grouping** in Settings for Audiobookshelf-compatible folder structure:
```
Author/Series/1 - Book Title/
```

### Will it mess up my library?

The app is designed to be safe:
- Drastic changes require manual approval
- Every fix can be undone
- Garbage matches are automatically rejected
- You can review everything before applying

### Does it move files or just rename folders?

It can move a complete book folder or a loose media file into a generated destination. The opt-in watch pre-sort can also combine recognized `CD`/`Disc`/`Part` sibling folders into a new unsuffixed folder before ingestion. That merge changes folder layout only; it does not join tracks or create an M4B. Existing destinations and ambiguous content remain blocked.

## Technical

### What APIs does it use?

Metadata can come from Skaldleita, Audnexus, OpenLibrary, Google Books, and Hardcover. Optional AI verification uses the configured Gemini, OpenRouter, Ollama, or OpenAI-compatible provider.

### Do I need to register for a Skaldleita key?

No. Library Manager `0.9.0-beta.168` and newer use a bundled Skaldleita key when
no personal key is saved. Shared access is limited to 300 requests/hour per
source IP. A personal key is optional and currently has the same hourly
allowance, but gives you a dedicated credential and email-based recovery.

Skaldleita still enforces IP blocks, signed-client verification, and its minimum
Library Manager version before authorizing either credential. A rejected
personal key is never retried with the shared key.

### What are the provider limits?

Hosted provider limits vary by account and model; check the provider's current documentation. Library Manager also applies its own configurable request limiter (default 200/hour, Settings clamp 10–500/hour).

### Can I use a local LLM?

Yes. Select Ollama or llama.cpp/OpenAI-compatible in Settings. Library Manager loads the model list from the selected server and also accepts a manually entered model ID.

### How does it know what's correct?

It cross-references multiple book databases, then uses AI to verify the best match. If there's uncertainty, it asks for human review.

### What is a transfer receipt?

Beta.161 develop builds create a durable source inventory with file sizes/types and SHA-256 hashes, verify the destination inventory, and record rollback status. History exposes the receipt and inventory modal. This improves auditability but is not a replacement for independent filesystem and hardware backups.

### How does verified pre-sort avoid losing files?

Beta.162 creates a literal inventory and destination mapping before the first move. It re-hashes each source immediately before apply, verifies every destination, records progress after each file, and verifies rollback after failures. It never overwrites an existing path and refuses ambiguous recovery. This protects the application handoff, but hardware failure, external edits, and storage corruption still require independent backups.

### Will pre-sort split chapter files into separate books?

Not automatically. Chapter-like files are treated as one audiobook. Explicit book/volume markers can produce a high-confidence split; generic distinct titles require review unless Skaldleita fingerprints identify them as different books. Shared or unassigned files block apply.

## Docker

### Do I need Docker?

No, you can run directly with Python. Docker is just convenient for some setups (UnRaid, Synology, etc.)

### Why can't I access my files in Docker?

Docker containers are isolated. You must mount your audiobook folder in docker-compose.yml:
```yaml
volumes:
  - /your/path:/audiobooks
```

See [[Docker Setup]] for details.

## Features

### Can it handle multiple libraries?

Yes! Add multiple paths in Settings (one per line), or mount multiple volumes in Docker.

### Does it preserve narrator versions?

Yes! If your folder has `(Narrator Name)` at the end, it keeps them separate.

### What about ebooks mixed with audiobooks?

The app detects ebook files and flags them but won't move them. You'll see a note in the issues.

### Can I exclude certain folders?

Folders like `metadata/`, `tmp/`, `cache/` are automatically skipped. For custom exclusions, you'd need to move them outside your library path for now.
