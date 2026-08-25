# Docker Setup

The canonical Docker walkthrough is [`DOCKER.md`](DOCKER.md). It covers volume mounts, Unraid, Synology, Windows/macOS, Dockge, Portainer, permissions, updates, and troubleshooting.

## Quick start

```bash
git clone https://github.com/deucebucket/library-manager.git
cd library-manager
# Edit docker-compose.yml and replace the host audiobook path.
docker compose up -d
```

Open `http://localhost:5757`. In Settings, use the container-side mount path (normally `/audiobooks`), not the host path. Persist `./data:/data` so settings and the database survive container replacement.

The root `docker-compose.yml` is the source of truth for the published image, port, health check, timezone, and optional `PUID`/`PGID` settings. Update this page only when the canonical guide or compose behavior changes.
