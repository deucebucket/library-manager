# Installation

## Quick Start (Python)

```bash
# Clone the repo
git clone https://github.com/deucebucket/library-manager.git
cd library-manager

# Install dependencies
python -m pip install -r requirements.txt

# Run
python app.py
```

Open **http://localhost:5757** in your browser.

## Docker

See [[Docker Setup]] for complete Docker instructions including UnRaid, Synology, and Portainer.

```bash
git clone https://github.com/deucebucket/library-manager.git
cd library-manager

# Edit docker-compose.yml with your audiobook path
docker compose up -d
```

## Requirements

- Python 3.9+ (for direct install; the current Docker image uses Python 3.11)
- Docker (for containerized install)
- For hosted AI, credentials for the selected provider when required:
  - [Google AI Studio](https://aistudio.google.com) (Gemini)
  - [OpenRouter](https://openrouter.ai) (multiple models available)
- Or a reachable Ollama, llama.cpp, or OpenAI-compatible server (local providers can run without a hosted key)

The default hosted Skaldleita metadata/audio service does not require signup in
`0.9.0-beta.168` or newer. Library Manager uses its bundled, per-IP-limited
shared credential when no personal Skaldleita key is saved. A custom/self-hosted
Skaldleita deployment must explicitly set `SKALDLEITA_LM_PUBLIC_KEY` to the
matching bundled credential or shared access is disabled. Saved personal values
live in `secrets.json`; Settings does not render them back to the browser and the
application does not log them.

## First Run

1. Open http://localhost:5757
2. Go to **Settings**
3. Add your **library path** (e.g., `/mnt/audiobooks`)
4. Configure the selected provider, model, endpoint, and credentials as applicable
5. Click **Save Settings**
6. Go to **Dashboard** → **Scan Library**

## Running as a Service

### Systemd (Linux)

```bash
sudo tee /etc/systemd/system/library-manager.service << 'EOF'
[Unit]
Description=Library Manager - Audiobook Organizer
After=network.target

[Service]
Type=simple
User=yourusername
WorkingDirectory=/path/to/library-manager
ExecStart=/usr/bin/python3 app.py
Restart=always
RestartSec=10

# Bound the app and all ffmpeg children. Raise these values if an explicitly
# configured local model needs more memory.
MemoryHigh=2G
MemoryMax=4G
MemorySwapMax=1G
OOMPolicy=continue

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable --now library-manager
```

The same memory settings are available as the checked-in
`systemd/library-manager-memory.conf` drop-in. Audio probes also default to a
2 GiB per-process address-space ceiling. Set
`LIBRARY_MANAGER_FFMPEG_MEMORY_MB` in the service environment only when a
verified workload requires a different value.

### Check Status

```bash
sudo systemctl status library-manager
```
