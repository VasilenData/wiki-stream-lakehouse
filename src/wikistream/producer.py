"""Publish the live Wikimedia recent-changes stream (server-sent events) to Kafka."""
from __future__ import annotations

import argparse
import json
import logging
import os
import time

import requests

from .schema import TOPIC, encode, slim

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
log = logging.getLogger("producer")


class OversizedEvent(ValueError):
    """The decoded SSE data buffer exceeded its documented limit."""


def sse_data(lines, max_event_bytes: int = 1024 * 1024):
    """Yield complete data frames; EOF never dispatches an unterminated event.

    Implements the WHATWG data-field framing subset. Event IDs/retry fields are
    intentionally ignored: this client does not implement a durable SSE cursor.
    """
    if max_event_bytes <= 0:
        raise ValueError("max_event_bytes must be positive")
    data, size = [], 0
    for index, line in enumerate(lines):
        if index == 0:
            line = line.removeprefix("\ufeff")
        if line == "":
            if data:
                yield "\n".join(data)
            data, size = [], 0
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            size += len(value.encode("utf-8")) + 1
            if size > max_event_bytes:
                raise OversizedEvent(f"SSE event exceeds {max_event_bytes} decoded bytes")
            data.append(value)


def stream_events(url: str = URL):
    """Parse framed UTF-8 events; back off on errors and empty/closed streams."""
    headers = {"User-Agent": os.getenv("WIKI_UA", "wikistream-portfolio/1.0 (https://github.com/VasilenData)"),
               "Accept": "text/event-stream"}
    backoff = 1
    while True:
        try:
            with requests.get(url, stream=True, headers=headers, timeout=(10, 60)) as r:
                r.raise_for_status()
                if r.status_code == 204:
                    return  # The server explicitly requested that this client stop.
                r.encoding = "utf-8"  # SSE always uses UTF-8, regardless of HTTP defaults.
                for payload in sse_data(r.iter_lines(decode_unicode=True)):
                    try:
                        record = json.loads(payload)
                    except json.JSONDecodeError:
                        log.warning("skipping malformed JSON SSE event")
                        continue
                    backoff = 1  # A successful HTTP handshake alone is not progress.
                    yield record
        except (requests.RequestException, OversizedEvent) as exc:
            log.warning("stream dropped (%s)", exc)
        log.info("stream closed; retrying in %ss", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


def publish(events, producer, seconds: int = 0) -> dict:
    """Count broker acknowledgements; a queued record is not yet delivered."""
    queued = delivered = skipped = 0
    errors = []
    started = time.monotonic()

    def acknowledge(error, message):
        nonlocal delivered
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    for raw in events:
        rec = slim(raw)
        if rec is None:
            skipped += 1
        else:
            key, value = encode(rec)
            producer.produce(TOPIC, key=key, value=value, on_delivery=acknowledge)
            queued += 1
        producer.poll(0)
        if errors or (seconds and time.monotonic() - started >= seconds):
            break
    outstanding = producer.flush(10)
    if errors or outstanding or delivered != queued:
        raise RuntimeError(f"Kafka delivery incomplete: queued={queued}, acknowledged={delivered}, "
                           f"outstanding={outstanding}, errors={len(errors)}")
    return {"queued": queued, "acknowledged": delivered, "skipped": skipped}


def main() -> None:
    from confluent_kafka import Producer

    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--seconds", type=int, default=0, help="stop after N seconds (0 = run forever)")
    a = ap.parse_args()
    if a.seconds < 0:
        ap.error("--seconds must be non-negative")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    # acks=all + idempotence: no duplicates from producer retries
    p = Producer({"bootstrap.servers": a.bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 50, "compression.type": "lz4"})
    started = time.monotonic()
    stats = publish(stream_events(), p, a.seconds)
    log.info("done acknowledged=%d queued=%d skipped=%d in %.0fs", stats["acknowledged"],
             stats["queued"], stats["skipped"], time.monotonic() - started)


if __name__ == "__main__":
    main()
