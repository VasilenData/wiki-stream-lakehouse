"""Read Spark 3.5 file-sink v1 commits, not arbitrary Parquet files on disk.

Local filesystem only. A compact log replaces earlier history. Missing/corrupt
metadata fails closed; an orphan data file is never evidence of a committed batch.
"""
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

LOG_NAME = re.compile(r"^(\d+)(\.compact)?$")


def committed_files(root: str | Path) -> list[str]:
    root = Path(root).resolve()
    logs = {}
    for path in (root / '_spark_metadata').iterdir():
        match = LOG_NAME.fullmatch(path.name)
        if match and path.is_file():
            batch = int(match[1])
            if batch not in logs or match[2]:
                logs[batch] = path
    if not logs:
        raise ValueError(f'No committed Spark batches in {root}')
    compact = [batch for batch, path in logs.items() if path.suffix == '.compact']
    start = max(compact, default=0)
    end = max(logs)
    if set(range(start, end + 1)) - logs.keys():
        raise ValueError(f'Incomplete Spark commit history in {root}')
    files = set()
    for batch in range(start, end + 1):
        lines = logs[batch].read_text(encoding='utf-8').splitlines()
        if not lines or lines[0] != 'v1':
            raise ValueError(f'Unsupported Spark commit format: {logs[batch]}')
        for line in lines[1:]:
            entry = json.loads(line)
            if not isinstance(entry, dict) or entry.get('action') not in {'add', 'delete'}:
                raise ValueError('Invalid Spark file action')
            uri = urlsplit(entry['path'])
            if uri.scheme not in {'', 'file'} or uri.netloc:
                raise ValueError('Only local Spark file paths are supported')
            path = Path(url2pathname(uri.path) if uri.scheme else unquote(uri.path)).resolve()
            if not path.is_relative_to(root) or path.suffix != '.parquet':
                raise ValueError('Committed path is outside the sink or is not Parquet')
            if entry['action'] == 'add':
                files.add(str(path))
            else:
                files.discard(str(path))
    if any(not Path(path).is_file() for path in files):
        raise FileNotFoundError('A committed Parquet file is missing; restore it before reading')
    return sorted(files)
