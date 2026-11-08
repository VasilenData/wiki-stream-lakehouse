"""Replay committed local raw Kafka payloads into a separate Kafka topic.

This preserves archived key/value bytes, not original micro-batch boundaries or
cross-partition order. Replayed event-time windows can therefore differ.
"""
import argparse

import duckdb

from .files import committed_files
from .schema import TOPIC


def replay(archive: str, producer, topic: str) -> dict:
    if not topic or topic == TOPIC:
        raise ValueError('Replay requires a separate, non-empty destination topic')
    files = committed_files(archive)
    if not files:
        raise ValueError('Archive has no committed records')
    queued = acknowledged = 0
    errors = []

    def delivered(error, message):
        nonlocal acknowledged
        if error is not None:
            errors.append(str(error))
        else:
            acknowledged += 1

    with duckdb.connect() as con:
        con.read_parquet(files).create_view('archive')
        if con.execute('select count(*) from archive where topic = ?', [topic]).fetchone()[0]:
            raise ValueError('Replay destination must differ from every archived source topic')
        rows = con.execute('select key, value from archive order by topic, partition, "offset"')
        while batch := rows.fetchmany(1000):
            for key, value in batch:
                producer.produce(topic, key=key, value=value, on_delivery=delivered)
                queued += 1
                producer.poll(0)
                if errors:
                    break
            if errors:
                break
    outstanding = producer.flush(10)
    if errors or outstanding or queued != acknowledged:
        raise RuntimeError(f'Replay incomplete: queued={queued}, acknowledged={acknowledged}, outstanding={outstanding}')
    return {'queued': queued, 'acknowledged': acknowledged}


def main():
    from confluent_kafka import Producer
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--topic', required=True)
    args = parser.parse_args()
    producer = Producer({'bootstrap.servers': args.bootstrap, 'enable.idempotence': True, 'acks': 'all'})
    print(replay(args.archive, producer, args.topic))


if __name__ == '__main__':
    main()
