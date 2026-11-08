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


def stream_events(url: str = URL):
    """Yield raw events; reconnect with backoff because SSE connections are dropped regularly."""
    headers = {"User-Agent": os.getenv("WIKI_UA", "wikistream-portfolio/1.0 (https://github.com/VasilenData)")}
    backoff = 1
    while True:
        try:
            with requests.get(url, stream=True, headers=headers, timeout=(10, 60)) as r:
                r.raise_for_status()
                backoff = 1
                for line in r.iter_lines(decode_unicode=True):
                    if line and line.startswith("data:"):
                        try:
                            yield json.loads(line[5:])
                        except json.JSONDecodeError:
                            continue
        except requests.RequestException as exc:
            log.warning("stream dropped (%s); retrying in %ss", exc, backoff)
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
