import json
from pathlib import Path
import random

import duckdb
import pytest

from wikistream.analytics import connect
from wikistream.files import committed_files
from wikistream.replay import replay


def log(root, batch, entries):
    metadata = root / '_spark_metadata'
    metadata.mkdir(parents=True, exist_ok=True)
    (metadata / str(batch)).write_text('v1\n' + '\n'.join(json.dumps(e) for e in entries), encoding='utf-8')


def entry(path, action='add'):
    return {'path': path.as_uri(), 'action': action}


def test_compaction_and_generated_commit_history_match_visible_file_set(tmp_path):
    rng = random.Random(501)
    paths = [tmp_path / f'part {n}.parquet' for n in range(20)]
    for path in paths:
        path.touch()
    expected = set()
    for batch in range(31):
        changes = []
        for _ in range(rng.randint(1, 5)):
            path = rng.choice(paths)
            action = rng.choice(['add', 'delete'])
            changes.append(entry(path, action))
            if action == 'add':
                expected.add(str(path))
            else:
                expected.discard(str(path))
        if batch % 10 == 9:
            log(tmp_path, f'{batch}.compact', [entry(Path(p)) for p in sorted(expected)])
            # Compacted prehistory can be removed entirely.
            for old in (tmp_path / '_spark_metadata').iterdir():
                if int(old.name.split('.')[0]) < batch:
                    old.unlink()
        else:
            log(tmp_path, batch, changes)
        assert committed_files(tmp_path) == sorted(expected)


def test_orphan_files_and_uncommitted_metadata_are_excluded(tmp_path):
    kept = tmp_path / 'kept.parquet'
    kept.touch()
    (tmp_path / 'orphan.parquet').touch()
    log(tmp_path, 0, [entry(kept)])
    (tmp_path / '_spark_metadata' / '.1.tmp').write_text('incomplete', encoding='utf-8')
    assert committed_files(tmp_path) == [str(kept)]


@pytest.mark.parametrize('damage', ['gap', 'missing_file', 'outside', 'version', 'remote'])
def test_incomplete_or_unsafe_metadata_fails_closed(tmp_path, damage):
    root = tmp_path / 'sink'
    root.mkdir()
    path = root / 'part.parquet'
    path.touch()
    log(root, 0, [entry(path)])
    if damage == 'gap':
        log(root, 2, [])
    elif damage == 'missing_file':
        path.unlink()
    elif damage == 'outside':
        log(root, 1, [entry(tmp_path / 'outside.parquet')])
    elif damage == 'remote':
        log(root, 1, [{'path': 's3://bucket/part.parquet', 'action': 'add'}])
    else:
        (root / '_spark_metadata/0').write_text('v99\n', encoding='utf-8')
    with pytest.raises((ValueError, FileNotFoundError)):
        committed_files(root)


def test_duckdb_queries_ignore_real_orphan_parquet_even_with_quoted_paths(tmp_path):
    lake = tmp_path / "reader's lake"
    with duckdb.connect() as con:
        for folder in ['bronze_events', 'silver_wiki_minute']:
            root = lake / folder
            root.mkdir(parents=True)
            committed = root / 'part.parquet'
            con.execute('copy (select 7 as value) to ? (format parquet)', [str(committed)])
            con.execute('copy (select 999 as value) to ? (format parquet)', [str(root / 'orphan.parquet')])
            log(root, 0, [entry(committed)])
    with connect(str(lake)) as con:
        assert con.execute('select * from bronze').fetchall() == [(7,)]
        assert con.execute('select * from silver').fetchall() == [(7,)]


class Producer:
    def __init__(self, failure=None):
        self.sent = []
        self.failure = failure

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        for *_, callback in self.sent:
            callback(self.failure, None)
        return 0


def test_replay_preserves_payloads_and_checks_acknowledgements(tmp_path):
    path = tmp_path / 'part.parquet'
    with duckdb.connect() as con:
        con.execute('create table records (topic varchar, partition integer, "offset" bigint, key blob, value blob)')
        records = [('source', 0, 1, b'k', b'{not-json'), ('source', 0, 0, None, b'\xff\x00')]
        con.executemany('insert into records values (?, ?, ?, ?, ?)', records)
        con.execute('copy records to ? (format parquet)', [str(path)])
    log(tmp_path, 0, [entry(path)])
    producer = Producer()
    assert replay(str(tmp_path), producer, 'replay-test') == {'queued': 2, 'acknowledged': 2}
    assert [r[:3] for r in producer.sent] == [('replay-test', None, b'\xff\x00'), ('replay-test', b'k', b'{not-json')]
    with pytest.raises(RuntimeError, match='Replay incomplete'):
        replay(str(tmp_path), Producer('not delivered'), 'replay-test')
    with pytest.raises(ValueError, match='destination'):
        replay(str(tmp_path), Producer(), 'source')
