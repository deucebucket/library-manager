"""Versioned read-only HTTP surface for music inspection."""
from __future__ import annotations

from flask import Blueprint, jsonify, request

from library_manager.config import load_config
from library_manager.music.inspect import inspect_payload

music_api_bp = Blueprint("music_api", __name__)


@music_api_bp.post("/api/v1/music/inspect")
def inspect_music():
    if not request.is_json:
        return jsonify({"schema": "music.inspection.error.v1", "state": "unavailable",
                        "error": "json_required"}), 415
    config = load_config()
    roots = config.get("music_library_roots", [])
    payload, status = inspect_payload(request.get_json(silent=True), roots)
    return jsonify(payload), status
