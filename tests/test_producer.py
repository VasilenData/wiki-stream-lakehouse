import pytest

from wikistream.producer import publish


class FakeProducer:
    def __init__(self, error=None, pending=0):
        self.error, self.pending, self.callbacks = error, pending, []

    def produce(self, topic, key, value, on_delivery):
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        for callback in self.callbacks[:len(self.callbacks) - self.pending]:
            callback(self.error, None)
        return self.pending


def event():
    return {"id": 1, "wiki": "enwiki", "type": "edit", "timestamp": 1700000000}


def test_success_counts_acknowledged_messages_and_skips_invalid_events():
    assert publish([event(), None], FakeProducer()) == {"queued": 1, "acknowledged": 1, "skipped": 1}


@pytest.mark.parametrize("producer", [FakeProducer(error="broker rejected message"), FakeProducer(pending=1)])
def test_delivery_error_or_incomplete_flush_cannot_report_success(producer):
    with pytest.raises(RuntimeError, match="delivery incomplete"):
        publish([event()], producer)


class Response:
    def __init__(self, lines=(), status=200):
        self.lines, self.status_code = lines, status
        self.encoding = None
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode):
        assert decode_unicode and self.encoding == "utf-8"
        return iter(self.lines)


def test_sse_multiline_unicode_comments_and_empty_data_fields():
    from wikistream.producer import sse_data

    lines = ["\ufeff: heartbeat", "id: cursor", "event: message", 'data: {"title": "Lisbão",',
             'data: "id": 7}', "", "data", "data:  leading space", "", "data: incomplete"]
    assert list(sse_data(lines)) == ['{"title": "Lisbão",\n"id": 7}', "\n leading space"]


def test_sse_eof_does_not_publish_partial_event_or_join_reconnections(monkeypatch):
    from wikistream import producer

    responses = [Response(['data: {"id": 1}', '']), Response(['data: {"id": 2}']),
                 Response(['data: {"id": 3,', 'data: "wiki": "enwiki"}', '']), Response(status=204)]
    pending = iter(responses)
    delays = []
    monkeypatch.setattr(producer.requests, "get", lambda *a, **kw: next(pending))
    monkeypatch.setattr(producer.time, "sleep", delays.append)
    assert list(producer.stream_events()) == [{"id": 1}, {"id": 3, "wiki": "enwiki"}]
    assert delays == [1, 2, 1] and all(r.closed for r in responses)


def test_empty_connections_back_off_with_cap_and_malformed_frame_recovers(monkeypatch):
    from wikistream import producer

    responses = iter([Response() for _ in range(7)] + [
        Response(['data: broken JSON', '', 'data: {"id": 8}', '']), Response(status=204)])
    delays = []
    monkeypatch.setattr(producer.requests, "get", lambda *a, **kw: next(responses))
    monkeypatch.setattr(producer.time, "sleep", delays.append)
    assert list(producer.stream_events()) == [{"id": 8}]
    assert delays == [1, 2, 4, 8, 16, 30, 30, 1]


def test_sse_buffer_limit_counts_utf8_bytes_and_releases_connection(monkeypatch):
    from wikistream import producer

    assert list(producer.sse_data(["data: é", ""], max_event_bytes=3)) == ["é"]
    with pytest.raises(producer.OversizedEvent):
        list(producer.sse_data(["data: é", ""], max_event_bytes=2))
    response = Response(["data: " + "x" * (1024 * 1024), ""])
    pending = iter([response, Response(status=204)])
    delays = []
    monkeypatch.setattr(producer.requests, "get", lambda *a, **kw: next(pending))
    monkeypatch.setattr(producer.time, "sleep", delays.append)
    assert list(producer.stream_events()) == []
    assert response.closed and delays == [1]
