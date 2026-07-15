# FAQ

## General

### Is this free?

Yes. The app is free and open source. Hosted AI providers use API keys:
- **Gemini** - Free, 14,400 calls/day
- **OpenRouter** - Free tier available

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

Just renames folders. Files inside stay exactly where they are - only the folder path changes.

## Technical

### What APIs does it use?

Metadata can come from Skaldleita, Audnexus, OpenLibrary, Google Books, and Hardcover. Optional AI verification uses the configured Gemini, OpenRouter, Ollama, or OpenAI-compatible provider.

### Why Gemini over GPT-4?

Gemini offers 14,400 free API calls per day, which is plenty for most libraries. GPT-4 would cost money for this volume.

### Can I use a local LLM?

Yes. Select Ollama or llama.cpp/OpenAI-compatible in Settings. Library Manager loads the model list from the selected server and also accepts a manually entered model ID.

### How does it know what's correct?

It cross-references multiple book databases, then uses AI to verify the best match. If there's uncertainty, it asks for human review.

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
