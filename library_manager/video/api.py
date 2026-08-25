"""Modular, read-only Plex reconciliation UI and API.

The blueprint deliberately receives no organization adapter or capability secret.  It
can show what Plex indexed versus disk, but cannot move a file or widen the authority of
the loopback-only video organization service.
"""
from __future__ import annotations

import os
from pathlib import Path
import sqlite3

from flask import Blueprint, current_app, jsonify, render_template

from library_manager.config import load_config
from plex_client import PlexDB


video_api_bp = Blueprint("video_api", __name__)
_SECTION_TYPES = {"movie": "movie_manager_enabled", "show": "tv_manager_enabled"}
_MAX_FINDINGS = 500


def _manager_state(config: dict) -> dict[str, bool]:
    return {
        "movies": bool(config.get("movie_manager_enabled", False)),
        "tv": bool(config.get("tv_manager_enabled", False)),
    }


def _client(config: dict) -> PlexDB:
    return PlexDB(str(config.get("plex_db_path") or "").strip() or None)


def _enabled(section_type: str, config: dict) -> bool:
    key = _SECTION_TYPES.get(section_type)
    return bool(key and config.get(key, False))


def _relative_path(path: str, roots: list[str]) -> str:
    """Return a display path without disclosing a configured host root."""
    normalized = os.path.normpath(path)
    for root in roots:
        try:
            relative = os.path.relpath(normalized, os.path.normpath(root))
        except (TypeError, ValueError):
            continue
        if relative != os.pardir and not relative.startswith(os.pardir + os.sep):
            return Path(relative).as_posix()
    return "[outside configured section]"


def _error(code: str, status: int):
    return jsonify({"schema": "plex.reconcile.error.v1", "status": "unavailable",
                    "error_code": code}), status


@video_api_bp.get("/video")
def video_dashboard():
    config = load_config()
    worker_status = current_app.extensions.get("video_worker_status", lambda: False)
    return render_template(
        "video_dashboard.html",
        config=config,
        managers=_manager_state(config),
        worker_running=bool(worker_status()),
    )


@video_api_bp.get("/api/plex/status")
def plex_status():
    config = load_config()
    managers = _manager_state(config)
    database_configured = bool(str(config.get("plex_db_path") or "").strip())
    available = False
    if database_configured and any(managers.values()):
        available = _client(config).available()
    return jsonify({
        "schema": "plex.reconcile.status.v1",
        "status": "ready" if available else "unavailable",
        "managers": managers,
        "database_configured": database_configured,
        "database_available": available,
    })


@video_api_bp.get("/api/plex/sections")
def plex_sections():
    config = load_config()
    if not any(_manager_state(config).values()):
        return _error("video_managers_disabled", 409)
    client = _client(config)
    if not client.available():
        return _error("plex_database_unavailable", 503)
    try:
        sections = [
            {"id": section.id, "name": section.name, "type": section.type,
             "root_count": len(section.locations)}
            for section in client.sections() if _enabled(section.type, config)
        ]
    except (OSError, ValueError, sqlite3.Error):
        return _error("plex_database_unavailable", 503)
    return jsonify({"schema": "plex.reconcile.sections.v1", "status": "ready",
                    "sections": sections})


@video_api_bp.get("/api/plex/reconcile/<int:section_id>")
def plex_reconcile(section_id: int):
    config = load_config()
    client = _client(config)
    if not client.available():
        return _error("plex_database_unavailable", 503)
    try:
        section = next((item for item in client.sections()
                        if item.id == section_id), None)
        if section is None:
            return _error("plex_section_unknown", 404)
        if not _enabled(section.type, config):
            return _error("video_manager_disabled", 409)
        result = client.reconcile(section)
    except (OSError, ValueError, sqlite3.Error):
        return _error("plex_reconciliation_unavailable", 503)

    stranded = [_relative_path(item, section.locations)
                for item in result.stranded[:_MAX_FINDINGS]]
    phantom = [_relative_path(item, section.locations)
               for item in result.phantom[:_MAX_FINDINGS]]
    return jsonify({
        "schema": "plex.reconcile.result.v1",
        "status": "ready",
        "section": {"id": section.id, "name": section.name,
                    "type": section.type},
        "counts": {"indexed": result.indexed, "on_disk": result.on_disk,
                   "stranded": len(result.stranded), "phantom": len(result.phantom)},
        "findings": {"stranded": stranded, "phantom": phantom},
        "truncated": (len(result.stranded) > _MAX_FINDINGS
                      or len(result.phantom) > _MAX_FINDINGS),
    })


__all__ = ["video_api_bp"]
