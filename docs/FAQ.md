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

It can move a complete book folder or a loose media file into a generated destination. It does not merge a non-empty destination folder; that is treated as a possible different version/narrator and is blocked.

## Technical

### What APIs does it use?

Metadata can come from Skaldleita, Audnexus, OpenLibrary, Google Books, and Hardcover. Optional AI verification uses the configured Gemini, OpenRouter, Ollama, or OpenAI-compatible provider.

### What are the provider limits?

Hosted provider limits vary by account and model; check the provider's current documentation. Library Manager also applies its own configurable request limiter (default 200/hour, Settings clamp 10–500/hour).

### Can I use a local LLM?

Yes. Select Ollama or llama.cpp/OpenAI-compatible in Settings. Library Manager loads the model list from the selected server and also accepts a manually entered model ID.

### How does it know what's correct?

It cross-references multiple book databases, then uses AI to verify the best match. If there's uncertainty, it asks for human review.

### What is a transfer receipt?

Beta.161 develop builds create a durable source inventory with file sizes/types and SHA-256 hashes, verify the destination inventory, and record rollback status. History exposes the receipt and inventory modal. This improves auditability but is not a replacement for independent filesystem and hardware backups.

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
