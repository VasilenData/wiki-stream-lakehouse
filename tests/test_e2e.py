"""End-to-end correctness test against a real Kafka broker and Spark.

Skipped unless KAFKA_BOOTSTRAP is set. It proves: dedupe, event-time windows, late-data
dropping and dead-lettering all behave as documented.
"""
import os
import shutil
import subprocess
import sys
import tempfile

import duckdb
import pytest

from wikistream.synthetic import scenario

BOOT = os.getenv("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOT, reason="set KAFKA_BOOTSTRAP to run the integration test")


def test_pipeline_end_to_end():
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    from wikistream.schema import TOPIC
    admin = AdminClient({"bootstrap.servers": BOOT})
    admin.delete_topics([TOPIC])[TOPIC].result() if TOPIC in admin.list_topics(timeout=10).topics else None
    import time
    time.sleep(3)
    admin.create_topics([NewTopic(TOPIC, 1, 1)])[TOPIC].result()
    msgs, phase2, expected = scenario()
    p = Producer({"bootstrap.servers": BOOT})

    def publish(batch):
        for k, v in batch:
            p.produce(TOPIC, key=k, value=v)
        p.flush()

    def run_job(d):
        subprocess.run([sys.executable, "-m", "wikistream.job", "--once", "--out", f"{d}/lake",
                        "--checkpoint", f"{d}/ck", "--bootstrap", BOOT], check=True, timeout=600)

    d = tempfile.mkdtemp()
    try:
        publish(msgs)
        run_job(d)           # batch 1: advances the watermark past minutes 0 and 1
        publish(phase2)      # late and malformed events arrive afterwards
        run_job(d)           # batch 2: same checkpoint, so the watermark is remembered
        con = duckdb.connect()
        rows = con.execute(f"""SELECT wiki, epoch(window_start)::BIGINT, edits, bot_edits
                               FROM read_parquet('{d}/lake/silver_wiki_minute/*.parquet')""").fetchall()
        got = {(w, s): (e, b) for w, s, e, b in rows}
        for key, val in expected.items():
            assert got[key] == val, (key, got)
        n_bronze = con.execute(f"SELECT count(*) FROM read_parquet('{d}/lake/bronze_events/**/*.parquet')").fetchone()[0]
        assert n_bronze == 6 + 4 + 1  # duplicates and late events removed
        shared = con.execute(f"SELECT wiki FROM read_parquet('{d}/lake/bronze_events/**/*.parquet') WHERE id = 1").fetchall()
        assert set(shared) == {("enwiki",), ("dewiki",)}  # rcid is local to a wiki
        n_dead = con.execute(f"SELECT count(*) FROM read_parquet('{d}/lake/dead_letters/*.parquet')").fetchone()[0]
        assert n_dead == 2
    finally:
        shutil.rmtree(d, ignore_errors=True)
