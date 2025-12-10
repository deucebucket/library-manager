# Docker Installation Guide

Complete guide for running Library Manager in Docker, with specific instructions for UnRaid, Synology, and standard Linux systems.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Understanding Docker Volumes](#understanding-docker-volumes)
- [Platform-Specific Setup](#platform-specific-setup)
  - [UnRaid](#unraid)
  - [Synology NAS](#synology-nas)
  - [Standard Linux](#standard-linux)
  - [Windows/Mac](#windowsmac)
- [Using Dockge](#using-dockge)
- [Using Portainer](#using-portainer)
- [Multiple Libraries](#multiple-libraries)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/deucebucket/library-manager.git
cd library-manager
```

### 2. Edit docker-compose.yml

Open `docker-compose.yml` and change the audiobook path:

```yaml
volumes:
  # Change this line to YOUR audiobook location
  - /path/to/your/audiobooks:/audiobooks
```

### 3. Start the Container

```bash
docker-compose up -d
```

### 4. Access the Web UI

Open **http://your-server-ip:5060** in your browser.

### 5. Configure Settings

1. Go to **Settings**
2. Set library path to: `/audiobooks` (this is the path INSIDE the container)
3. Add your API key (Gemini recommended)
4. Save and start scanning!

---

## Understanding Docker Volumes

**This is the most important concept to understand.**

Docker containers are isolated - they cannot see your files unless you explicitly share them via **volume mounts**.

### How It Works

```
YOUR SERVER                      DOCKER CONTAINER
────────────────────────────────────────────────────
/mnt/audiobooks/                 (invisible)
/home/user/books/                (invisible)
/media/storage/audio/            (invisible)

With volume mount in docker-compose.yml:
- /mnt/audiobooks:/audiobooks

/mnt/audiobooks/          →      /audiobooks ✓
```

### The Two Paths

| Path Type | Example | Where to Use |
|-----------|---------|--------------|
| **Host path** | `/mnt/user/media/audiobooks` | docker-compose.yml (left side of `:`) |
| **Container path** | `/audiobooks` | Settings page in the web UI |

### Common Mistake

```
❌ WRONG: Skip volume mount, type host path in Settings
   Result: "Path doesn't exist" error

✅ RIGHT: Add volume mount in compose, use container path in Settings
```

---

## Platform-Specific Setup

### UnRaid

UnRaid stores shares at `/mnt/user/sharename`.

**docker-compose.yml:**
```yaml
volumes:
  # UnRaid user share
  - /mnt/user/media/audiobooks:/audiobooks

  # Or direct disk path
  - /mnt/disk1/audiobooks:/audiobooks

  # Data persistence
  - /mnt/user/appdata/library-manager:/data
```

**Settings page:** Set library path to `/audiobooks`

**Tips:**
- Use `/mnt/user/` paths for shares (recommended)
- Use `/mnt/diskX/` for direct disk access
- Store app data in `/mnt/user/appdata/` for easy backups

---

### Synology NAS

Synology volumes are at `/volume1/`, `/volume2/`, etc.

**docker-compose.yml:**
```yaml
volumes:
  # Synology shared folder
  - /volume1/media/audiobooks:/audiobooks

  # Data persistence
  - /volume1/docker/library-manager:/data
```

**Settings page:** Set library path to `/audiobooks`

**Tips:**
- Find your volume number in Control Panel → Shared Folder
- Create a `docker` shared folder for app data
- Use Docker package from Package Center, or install via SSH

---

### Standard Linux

**docker-compose.yml:**
```yaml
volumes:
  # Your audiobook directory
  - /home/username/audiobooks:/audiobooks

  # Or mounted drive
  - /media/storage/audiobooks:/audiobooks

  # Data persistence (same directory as compose file)
  - ./data:/data
```

**Settings page:** Set library path to `/audiobooks`

**Tips:**
- Use absolute paths (starting with `/`)
- Make sure the user running Docker can read/write the audiobook directory
- `./data` creates a `data` folder next to your docker-compose.yml

---

### Windows/Mac

Docker Desktop handles path translation automatically.

**Windows docker-compose.yml:**
```yaml
volumes:
  # Windows path (note the forward slashes)
  - C:/Users/YourName/Audiobooks:/audiobooks

  # Or WSL path
  - /mnt/c/Users/YourName/Audiobooks:/audiobooks

  - ./data:/data
```

**Mac docker-compose.yml:**
```yaml
volumes:
  - /Users/yourname/Audiobooks:/audiobooks
  - ./data:/data
```

**Tips:**
- Docker Desktop must have access to the drive (Settings → Resources → File Sharing)
- Use forward slashes even on Windows
- Performance is better with WSL2 backend on Windows

---

## Using Dockge

[Dockge](https://github.com/louislam/dockge) is a Docker compose manager with a nice UI.

### Setup Steps

1. In Dockge, click **+ Compose**
2. Give it a name: `library-manager`
3. Paste this compose content:

```yaml
version: "3.8"

services:
  library-manager:
    build: https://github.com/deucebucket/library-manager.git
    container_name: library-manager
    restart: unless-stopped
    ports:
      - "5060:5060"
    volumes:
      # CHANGE THIS to your audiobook path
      - /mnt/user/media/audiobooks:/audiobooks
      - ./data:/data
    environment:
      - TZ=America/Chicago
```

4. Click **Deploy**
5. Access at http://your-server:5060

---

## Using Portainer

### Setup Steps

1. Go to **Stacks** → **Add Stack**
2. Name: `library-manager`
3. Select **Repository** and enter:
   - Repository URL: `https://github.com/deucebucket/library-manager`
   - Compose path: `docker-compose.yml`
4. Under **Environment variables**, you can override settings
5. Click **Deploy the stack**

### Or Use Web Editor

Paste the compose content directly and edit the volume paths.

---

## Multiple Libraries

You can mount multiple audiobook folders:

```yaml
volumes:
  # Multiple libraries
  - /mnt/disk1/audiobooks:/audiobooks
  - /mnt/disk2/more-audiobooks:/library2
  - /mnt/disk3/old-audiobooks:/library3

  # Data persistence
  - ./data:/data
```

Then in **Settings**, add all paths (one per line):
```
/audiobooks
/library2
/library3
```

---

## Troubleshooting

### "Path doesn't exist" Error

**Cause:** You entered a host path in Settings instead of the container path.

**Fix:**
1. Check your docker-compose.yml volume mount
2. Use the RIGHT side of the `:` in Settings (e.g., `/audiobooks`)

### Permission Denied

**Cause:** Container user can't read/write your audiobook files.

**Fix for Linux:**
```bash
# Find your user/group IDs
id

# Make sure audiobook folder is accessible
chmod -R 755 /path/to/audiobooks
```

**Fix for UnRaid:** Files should be accessible by default. Check share permissions.

### Container Won't Start

**Check logs:**
```bash
docker logs library-manager
```

**Common issues:**
- Port 5060 already in use → Change to `5061:5060`
- Volume path doesn't exist → Create the directory first
- Syntax error in compose file → Validate YAML

### Can't Connect to Web UI

1. Check container is running: `docker ps`
2. Check port mapping: `docker port library-manager`
3. Try `http://localhost:5060` if on same machine
4. Check firewall allows port 5060

### Changes Not Persisting

**Cause:** Data volume not mounted correctly.

**Fix:** Make sure `./data:/data` is in your volumes and the `data` folder exists:
```bash
mkdir -p data
docker-compose down
docker-compose up -d
```

### Container Can't See New Files

Docker doesn't auto-refresh mounts. If you add new audiobooks:
1. They should appear automatically (no restart needed)
2. If not, check the host path is correct
3. Run a new scan from the web UI

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TZ` | `UTC` | Timezone (e.g., `America/New_York`) |
| `DATA_DIR` | `/data` | Where config/database are stored |

---

## Updating

```bash
cd library-manager
git pull
docker-compose build --no-cache
docker-compose up -d
```

Or with Dockge/Portainer, redeploy the stack.

---

## Getting Help

- **GitHub Issues:** [Report a bug](https://github.com/deucebucket/library-manager/issues)
- **Check logs:** `docker logs library-manager`
- **Enter container:** `docker exec -it library-manager /bin/bash`
