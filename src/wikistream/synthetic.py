"""Deterministic synthetic events with known answers, used by the end-to-end correctness test."""
from __future__ import annotations

import json

from .schema import TOPIC, encode

BASE = 1_800_000_000  # a fixed epoch second aligned to a minute boundary (divisible by 60)
assert BASE % 60 == 0


def make(i: int, wiki: str, ts: int, bot: bool = False, delta: int = 100) -> dict:
    return {"id": i, "wiki": wiki, "type": "edit", "bot": bot, "user": f"u{i % 5}", "title": f"T{i}",
            "bytes_delta": delta, "event_ts": ts}


def scenario() -> tuple[list[tuple[bytes, bytes]], list[tuple[bytes, bytes]], dict]:
    """Returns messages and the expected silver result.

    Minute 0: 6 unique edits on enwiki (2 bots) + 3 duplicates of ids already sent.
    Minute 1: 4 edits on dewiki.
    Then an event at +10 minutes advances the watermark (2 min), so minutes 0 and 1 close.
    Phase 2 (a later micro-batch, because Spark advances the watermark between batches):
    2 LATE events for minute 0 (older than the watermark) which must be dropped, plus
    2 malformed messages which must land in dead letters.
    """
    msgs, uid = [], 0
    for k in range(6):
        uid += 1
        msgs.append(encode(make(uid, "enwiki", BASE + 5 * k, bot=k < 2)))
    for dup in (1, 2, 3):
        msgs.append(encode(make(dup, "enwiki", BASE + 5 * (dup - 1), bot=dup <= 2)))
    for k in range(4):
        uid += 1
        msgs.append(encode(make(uid, "dewiki", BASE + 60 + 10 * k)))
    uid += 1
    msgs.append(encode(make(uid, "enwiki", BASE + 600)))  # advances the watermark
    phase2 = [encode(make(1000 + k, "enwiki", BASE + 20)) for k in range(2)]  # late
    phase2.append((b"bad", b"{not json"))
    phase2.append((b"bad2", json.dumps({"wiki": "enwiki"}).encode()))  # valid JSON, missing id/event_ts
    expected = {("enwiki", BASE): (6, 2), ("dewiki", BASE + 60): (4, 0)}
    return msgs, phase2, expected


__all__ = ["TOPIC", "scenario", "BASE"]
