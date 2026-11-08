"""Pure functions that turn raw Wikimedia events into the compact record we publish to Kafka.

Kept free of Spark and Kafka imports so they can be unit tested in milliseconds.
"""
from __future__ import annotations

import json
from typing import Any

TOPIC = "wiki.recentchange"
REQUIRED = ("id", "wiki", "type", "event_ts")


def slim(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Reduce a Wikimedia `recentchange` event to the fields we use.

    Returns None for events that cannot be used (no id, wiki or timestamp), so the producer
    never publishes garbage. Log events have no length, so byte delta defaults to 0.
    """
    if not isinstance(raw, dict):
        return None
    if any(not isinstance(raw.get(key), str) or not raw[key].strip() for key in ("wiki", "type")):
        return None
    if any(type(raw.get(key)) is not int or not 0 < raw[key] < 2**63 for key in ("id", "timestamp")):
        return None
    if raw.get("length") is not None and not isinstance(raw["length"], dict):
        return None
    try:
        length = raw.get("length") or {}
        rec = {
            "id": int(raw["id"]),
            "wiki": str(raw["wiki"]),
            "type": str(raw["type"]),
            "bot": bool(raw.get("bot", False)),
            "user": str(raw.get("user", "")),
            "title": str(raw.get("title", ""))[:300],
            "bytes_delta": int(length.get("new", 0) or 0) - int(length.get("old", 0) or 0),
            "event_ts": int(raw["timestamp"]),
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    return rec


def encode(rec: dict[str, Any]) -> tuple[bytes, bytes]:
    """Kafka key and value. Keying by wiki keeps one wiki's events ordered within a partition."""
    return rec["wiki"].encode(), json.dumps(rec, separators=(",", ":")).encode()


def is_valid(rec: dict[str, Any] | None) -> bool:
    return rec is not None and all(rec.get(k) is not None for k in REQUIRED)
