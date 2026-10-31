from wikistream.schema import encode, is_valid, slim
from wikistream.synthetic import BASE, scenario


def raw(**kw):
    d = {"id": 7, "wiki": "enwiki", "type": "edit", "bot": True, "user": "x", "title": "T",
         "timestamp": 1700000000, "length": {"old": 100, "new": 160}}
    d.update(kw)
    return d


def test_slim_computes_byte_delta():
    assert slim(raw())["bytes_delta"] == 60


def test_slim_handles_log_events_without_length():
    r = raw(type="log")
    r.pop("length")
    assert slim(r)["bytes_delta"] == 0


def test_slim_rejects_events_missing_required_fields():
    for k in ("id", "wiki", "timestamp"):
        r = raw()
        r.pop(k)
        assert slim(r) is None


def test_slim_rejects_non_numeric_id():
    assert slim(raw(id="abc")) is None


def test_title_is_truncated():
    assert len(slim(raw(title="x" * 1000))["title"]) == 300


def test_encode_keys_by_wiki():
    k, v = encode(slim(raw()))
    assert k == b"enwiki" and b'"id":7' in v


def test_is_valid():
    assert is_valid(slim(raw())) and not is_valid(None)


def test_scenario_is_consistent():
    msgs, phase2, expected = scenario()
    assert len(msgs) == 6 + 3 + 4 + 1 and len(phase2) == 4
    assert BASE % 60 == 0 and sum(v[0] for v in expected.values()) == 10
