"""Opt-in failure exercise: kill/restart the disposable CI broker, retaining its data.

Never point KAFKA_OUTAGE_TEST_DIR at a broker used for other work. The test checks
the recorded PID's command line before sending SIGKILL. Timing is an observation
of this single-node exercise, not a production availability or durability claim.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

import duckdb
import pytest

from wikistream.files import committed_files
from wikistream.schema import encode
from wikistream.synthetic import BASE, make

BROKER_DIR = os.getenv('KAFKA_OUTAGE_TEST_DIR')
pytestmark = pytest.mark.skipif(not BROKER_DIR or sys.platform != 'linux',
                                reason='explicit disposable Linux broker required')


def snapshot(lake):
    """Read committed rows, retaining identities for independent loss checks."""
    with duckdb.connect() as con:
        for name, folder in [('raw', 'raw_kafka'), ('bronze', 'bronze_events'), ('dead', 'dead_letters')]:
            con.read_parquet(committed_files(lake / folder)).create_view(name)
        raw = con.execute('select topic, partition, "offset" from raw').fetchall()
        bronze = con.execute('select wiki, id from bronze').fetchall()
        dead = con.execute('select source_topic, source_partition, source_offset from dead').fetchall()
    return {'raw': raw, 'bronze': bronze, 'dead': dead}


def test_broker_outage_recovers_acknowledged_records(tmp_path):
    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    directory = Path(BROKER_DIR).resolve()
    config = directory / 'kraft.properties'
    pid = int((directory / 'kafka.pid').read_text())
    command_line = Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ').decode()
    assert 'kafka.Kafka' in command_line and str(config) in command_line, 'PID is not this test broker'
    server = directory / 'kafka_2.13-3.7.1/bin/kafka-server-start.sh'
    assert server.is_file() and config.is_file()
    bootstrap = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
    topic = 'wikistream-broker-outage'
    admin = AdminClient({'bootstrap.servers': bootstrap, 'socket.timeout.ms': 1000})
    assert topic not in admin.list_topics(timeout=10).topics, 'use a newly initialized disposable broker'
    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result()
    evidence_dir = Path(os.getenv('WIKISTREAM_RECOVERY_ARTIFACT_DIR', str(tmp_path / 'evidence')))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    report = {'schema_version': 1, 'status': 'failed',
              'exercise': 'single-broker process SIGKILL with retained Kafka data and Spark checkpoints',
              'started_at': datetime.now(timezone.utc).isoformat(),
              'revision': os.getenv('GITHUB_SHA'), 'run_id': os.getenv('GITHUB_RUN_ID'),
              'broker_version': '3.7.1', 'spark_version': '3.5.3',
              'scope': 'Synthetic local CI traffic; process failure only, not host/disk loss or HA.',
              'spark_restarts': 0}
    acknowledged, delivery_errors = [], []
    producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True,
                         'message.timeout.ms': 120000, 'socket.timeout.ms': 1000})

    def enqueue(messages):
        for key, value, kind in messages:
            def delivered(error, message, kind=kind):
                if error:
                    delivery_errors.append(str(error))
                else:
                    acknowledged.append({'identity': (message.topic(), message.partition(), message.offset()),
                                         'kind': kind})
            producer.produce(topic, key=key, value=value, on_delivery=delivered)

    def valid(i, second, kind='valid'):
        key, value = encode(make(i, 'enwiki', BASE + second))
        return key, value, kind

    initial = [valid(i, i) for i in range(1, 13)] + [valid(i, i, 'duplicate') for i in (1, 2)]
    initial += [(b'bad1', b'{', 'rejected'), (b'bad2', b'{}', 'rejected')]
    during = [valid(i, 60 + i - 100) for i in range(100, 110)]
    during += [valid(100, 60, 'duplicate'), (b'bad3', b'{', 'rejected')]
    lake, checkpoint = tmp_path / 'lake', tmp_path / 'checkpoint'
    cmd = [sys.executable, '-m', 'wikistream.job', '--bootstrap', bootstrap, '--topic', topic,
           '--out', str(lake), '--checkpoint', str(checkpoint), '--trigger', '1 second']
    spark = broker = None
    spark_log = (evidence_dir / 'spark.log').open('w', encoding='utf-8')
    broker_log = (evidence_dir / 'restarted-broker.log').open('w', encoding='utf-8')

    def launch_spark():
        return subprocess.Popen(cmd, stdout=spark_log, stderr=subprocess.STDOUT, start_new_session=True)

    def wait_for_rows(expected, timeout=180, allow_restart=False):
        nonlocal spark
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if spark.poll() is not None:
                assert allow_restart, 'Spark exited before the broker outage'
                assert report['spark_restarts'] == 0, 'Spark failed again after checkpoint restart'
                report['spark_restarts'] += 1
                spark = launch_spark()
            try:
                observed = snapshot(lake)
                if {key: len(value) for key, value in observed.items()} == expected:
                    return observed
            except (FileNotFoundError, ValueError, duckdb.Error):
                pass  # Sinks may not have published their first committed batch yet.
            time.sleep(0.2)
        pytest.fail(f'Committed sinks did not reach expected counts: {expected}')

    try:
        enqueue(initial)
        assert producer.flush(30) == 0 and not delivery_errors
        assert len(acknowledged) == len(initial)
        spark = launch_spark()
        wait_for_rows({'raw': 16, 'bronze': 12, 'dead': 2})
        assert spark.poll() is None
        report['baseline_committed_rows'] = 16
        interrupted_at = time.monotonic()
        os.kill(pid, signal.SIGKILL)
        # Prove the outage through a failed broker metadata call, not a sleep alone.
        deadline = time.monotonic() + 10
        while True:
            try:
                admin.list_topics(timeout=2)
            except KafkaException:
                break
            assert time.monotonic() < deadline, 'broker still answers after SIGKILL'
        enqueue(during)
        until = time.monotonic() + 5
        while time.monotonic() < until:
            producer.poll(0.1)
        assert len(acknowledged) == len(initial), 'outage messages were acknowledged while broker was down'
        report['queued_during_outage'] = len(during)
        restart_at = time.monotonic()
        broker = subprocess.Popen([str(server), str(config)], stdout=broker_log, stderr=subprocess.STDOUT,
                                  env={**os.environ, 'KAFKA_HEAP_OPTS': '-Xmx1g'}, start_new_session=True)
        (directory / 'kafka.pid').write_text(str(broker.pid) + '\n')
        deadline = time.monotonic() + 120
        while True:
            assert broker.poll() is None, 'restarted broker exited; inspect restarted-broker.log'
            try:
                if topic in admin.list_topics(timeout=1).topics:
                    break
            except KafkaException:
                pass
            assert time.monotonic() < deadline, 'broker failed to recover metadata service'
            time.sleep(0.2)
        broker_ready_at = time.monotonic()
        assert producer.flush(60) == 0 and not delivery_errors
        assert len(acknowledged) == len(initial) + len(during)
        observed = wait_for_rows({'raw': 28, 'bronze': 22, 'dead': 3}, allow_restart=True)
        recovered_at = time.monotonic()
        expected_offsets = {item['identity'] for item in acknowledged}
        expected_rejections = {item['identity'] for item in acknowledged if item['kind'] == 'rejected'}
        actual_offsets = set(observed['raw'])
        expected_ids = {('enwiki', i) for i in [*range(1, 13), *range(100, 110)]}
        report.update({'acknowledged_source_records': len(acknowledged),
                       'raw_records': len(observed['raw']), 'accepted_unique_events': len(observed['bronze']),
                       'rejected_records': len(observed['dead']), 'intentional_duplicate_inputs': 3,
                       'missing_acknowledged_offsets': len(expected_offsets - actual_offsets),
                       'unexpected_offsets': len(actual_offsets - expected_offsets),
                       'duplicate_raw_offsets': len(observed['raw']) - len(actual_offsets),
                       'duplicate_accepted_keys': len(observed['bronze']) - len(set(observed['bronze'])),
                       'broker_unavailable_seconds': round(broker_ready_at - interrupted_at, 6),
                       'broker_restart_seconds': round(broker_ready_at - restart_at, 6),
                       'recovery_seconds_from_broker_restart': round(recovered_at - restart_at, 6),
                       'end_to_end_interruption_seconds': round(recovered_at - interrupted_at, 6),
                       'recovery_definition': 'Time until all acknowledged source offsets and expected accepted/rejected rows are committed.'})
        assert len(expected_offsets) == len(acknowledged)
        assert actual_offsets == expected_offsets and len(observed['raw']) == len(actual_offsets)
        assert set(observed['bronze']) == expected_ids and len(observed['bronze']) == len(expected_ids)
        assert set(observed['dead']) == expected_rejections and len(observed['dead']) == len(expected_rejections)
        report['status'] = 'passed'
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        report['delivery_errors'] = delivery_errors
        (evidence_dir / 'broker-recovery.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(report, sort_keys=True))
        for process in (spark, broker):
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
        spark_log.close()
        broker_log.close()
