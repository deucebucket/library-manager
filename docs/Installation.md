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

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable --now library-manager
```

### Check Status

```bash
sudo systemctl status library-manager
```
