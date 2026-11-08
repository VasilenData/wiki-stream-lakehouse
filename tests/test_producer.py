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
