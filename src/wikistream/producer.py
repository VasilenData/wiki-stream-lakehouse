"""Publish the live Wikimedia recent-changes stream (server-sent events) to Kafka."""
from __future__ import annotations

import argparse
import json
import logging
import os
import time

import requests
from confluent_kafka import Producer

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--seconds", type=int, default=0, help="stop after N seconds (0 = run forever)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    # acks=all + idempotence: no duplicates from producer retries
    p = Producer({"bootstrap.servers": a.bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 50, "compression.type": "lz4"})
    sent = skipped = 0
    t0 = time.time()
    for raw in stream_events():
        rec = slim(raw)
        if rec is None:
            skipped += 1
        else:
            k, v = encode(rec)
            p.produce(TOPIC, key=k, value=v)
            sent += 1
        p.poll(0)
        if sent and sent % 500 == 0:
            log.info("sent=%d skipped=%d rate=%.1f/s", sent, skipped, sent / (time.time() - t0))
        if a.seconds and time.time() - t0 > a.seconds:
            break
    p.flush(10)
    log.info("done sent=%d skipped=%d in %.0fs", sent, skipped, time.time() - t0)


if __name__ == "__main__":
    main()
