"""End-to-end correctness test against a real Kafka broker and Spark.

Skipped unless KAFKA_BOOTSTRAP is set. It proves: dedupe, event-time windows, late-data
dropping and dead-lettering all behave as documented.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import signal
import time
from pathlib import Path

import duckdb
import pytest

from wikistream.synthetic import scenario
from wikistream.files import committed_files

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
        con.read_parquet(committed_files(f'{d}/lake/silver_wiki_minute')).create_view('silver')
        con.read_parquet(committed_files(f'{d}/lake/bronze_events')).create_view('bronze')
        con.read_parquet(committed_files(f'{d}/lake/dead_letters')).create_view('dead')
        con.read_parquet(committed_files(f'{d}/lake/raw_kafka')).create_view('raw_kafka')
        rows = con.execute("SELECT wiki, epoch(window_start)::BIGINT, edits, bot_edits FROM silver").fetchall()
        got = {(w, s): (e, b) for w, s, e, b in rows}
        for key, val in expected.items():
            assert got[key] == val, (key, got)
        n_bronze = con.execute('SELECT count(*) FROM bronze').fetchone()[0]
        assert n_bronze == 6 + 4 + 1  # duplicates and late events removed
        shared = con.execute('SELECT wiki FROM bronze WHERE id = 1').fetchall()
        assert set(shared) == {("enwiki",), ("dewiki",)}  # rcid is local to a wiki
        n_dead = con.execute('SELECT count(*) FROM dead').fetchone()[0]
        assert n_dead == 2
        assert con.execute('SELECT count(*) FROM raw_kafka').fetchone()[0] == len(msgs) + len(phase2)
        assert con.execute('SELECT count(distinct (topic, partition, "offset")) FROM raw_kafka').fetchone()[0] == 18
        con.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.skipif(os.name != 'posix', reason='CI uses POSIX process-group SIGKILL, including the Spark JVM')
def test_inflight_crash_recovery_and_missing_offsets_fail_closed(tmp_path):
    """Kill at an observed offset-log/commit-log barrier, not after a fixed delay."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    from wikistream.schema import TOPIC, encode
    from wikistream.synthetic import BASE, make

    admin = AdminClient({'bootstrap.servers': BOOT})
    if TOPIC in admin.list_topics(timeout=10).topics:
        admin.delete_topics([TOPIC])[TOPIC].result()
        time.sleep(3)
    admin.create_topics([NewTopic(TOPIC, 1, 1)])[TOPIC].result()
    producer = Producer({'bootstrap.servers': BOOT})

    def send(messages):
        for key, value in messages:
            producer.produce(TOPIC, key=key, value=value)
        assert producer.flush(30) == 0

    lake, checkpoint = tmp_path / 'lake', tmp_path / 'checkpoint'
    cmd = [sys.executable, '-m', 'wikistream.job', '--bootstrap', BOOT,
           '--out', str(lake), '--checkpoint', str(checkpoint)]
    initial, _, _ = scenario()
    send(initial)
    subprocess.run([*cmd, '--once'], check=True, timeout=600)
    with duckdb.connect() as con:
        assert con.read_parquet(committed_files(lake / 'raw_kafka')).count('*').fetchone()[0] == len(initial)
    previous = [int(p.name) for p in (checkpoint / 'raw_kafka/commits').iterdir() if p.name.isdigit()]
    next_batch = max(previous) + 1
    new = [encode(make(2000 + i, 'enwiki', BASE + 700)) for i in range(1000)]
    new.append(encode(make(5000, 'enwiki', BASE + 1500)))
    send(new)
    log_path = tmp_path / 'interrupted-driver.log'
    with log_path.open('w', encoding='utf-8') as log:
        process = subprocess.Popen([*cmd, '--max-offsets', '100'], stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        deadline = time.monotonic() + 240
        try:
            while time.monotonic() < deadline:
                assert process.poll() is None, log_path.read_text(encoding='utf-8')[-8000:]
                offsets = checkpoint / 'raw_kafka/offsets' / str(next_batch)
                commit = checkpoint / 'raw_kafka/commits' / str(next_batch)
                if offsets.exists() and not commit.exists():
                    os.killpg(process.pid, signal.SIGKILL)
                    break
                if commit.exists():
                    next_batch += 1
                time.sleep(0.01)  # Poll a durable batch-state condition, not a timing guess.
            else:
                pytest.fail('Never observed an in-flight raw batch')
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=30)
    assert process.returncode != 0
    subprocess.run([*cmd, '--once'], check=True, timeout=600)
    files = committed_files(lake / 'bronze_events')
    # A crash can leave data files outside the commit manifest. Inject one even
    # if this particular SIGKILL happened before a task wrote its first file.
    shutil.copy2(files[0], Path(files[0]).with_name('orphan-from-uncommitted-task.parquet'))
    with duckdb.connect() as con:
        con.read_parquet(committed_files(lake / 'raw_kafka')).create_view('raw_kafka')
        con.read_parquet(committed_files(lake / 'bronze_events')).create_view('bronze')
        assert con.execute('select count(*) from raw_kafka').fetchone()[0] == len(initial) + len(new)
        assert con.execute('select count(distinct (topic, partition, "offset")) from raw_kafka').fetchone()[0] == len(initial) + len(new)
        assert con.execute('select count(*) from bronze').fetchone()[0] == 11 + len(new)
        assert con.execute('select count(distinct (wiki,id)) from bronze').fetchone()[0] == 11 + len(new)

    # Recreate the source with fewer offsets: continuing from this checkpoint
    # must report data loss, not silently start again at zero.
    admin.delete_topics([TOPIC])[TOPIC].result()
    time.sleep(3)
    admin.create_topics([NewTopic(TOPIC, 1, 1)])[TOPIC].result()
    producer = Producer({'bootstrap.servers': BOOT})
    send([encode(make(9000, 'enwiki', BASE + 2000))])
    failed = subprocess.run([*cmd, '--once'], capture_output=True, text=True, timeout=600)
    assert failed.returncode != 0
    assert 'failOnDataLoss' in failed.stdout + failed.stderr
