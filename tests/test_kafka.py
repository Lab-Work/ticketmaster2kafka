"""Kafka delivery visibility (TM-03) and the unchanged wire format.

lv_kafka_connector's produce() only ENQUEUES. A missing topic, an ACL denial or a dead
broker is invisible to produce() and to flush()'s return value; the only truthful signal
is the per-message delivery callback, which librdkafka runs when somebody calls
poll()/flush(). The old code passed no callback, so the service logged "Produced N" and
counted them ok while Kafka refused every message. FakeKafka (conftest.py) behaves the
same way: it swallows enqueue errors unless raise_on_error=True and runs callbacks only
at poll()/flush().
"""

from __future__ import annotations

import json
import logging
import threading

import pytest

import ticketmaster2kafka as t
from conftest import FakeKafkaError, FakeMessage, make_event


@pytest.fixture
def tm(make_producer):
    return make_producer(dict(t.DEFAULT_QUERY_PARAMS))


def events(n):
    return [make_event(f"e{i}") for i in range(n)]


# ----- every produce() carries the callback and raise_on_error --------------------


def test_every_produce_passes_on_delivery_and_raise_on_error(tm, kafka):
    tm.produce_events_to_kafka(events(5))
    assert len(kafka.produced) == 5
    for m in kafka.produced:
        assert m["on_delivery"] == tm._on_delivery
        assert m["raise_on_error"] is True


def test_the_wire_format_is_unchanged(tm, kafka):
    ev = make_event("e1", name='Café "Quoted" \\ back–slash')
    tm.produce_events_to_kafka([ev])
    (m,) = kafka.produced
    assert m["topic"] == "nashville.ticketmaster.events"
    assert m["key"] == "0"
    assert m["headers"] == {"service": b"ticketmaster", "datatype": b"event"}
    assert isinstance(m["value"], str)
    # The service hands the connector json.dumps(payload), a str; the connector encodes
    # a str once more on the wire, which is the "JSON string holding JSON" consumers see.
    payload = json.loads(m["value"])
    assert list(payload) == ["source", "fetched_at", "event"]
    assert payload["source"] == "ticketmaster" and payload["event"] == ev
    assert m["value"] == json.dumps(
        {"source": "ticketmaster", "fetched_at": payload["fetched_at"], "event": ev})


# ----- delivery results ------------------------------------------------------------


def test_the_metric_help_texts_say_what_the_counters_really_count(tm, tel):
    """events_emitted_total counts messages ENQUEUED, not delivered, and the help text is
    where a dashboard reader learns that: "emitted" alone reads as "arrived". Reverting it
    to the template's "Events produced to kafka." makes a flat events_delivered_total next
    to a rising events_emitted_total look like a bug instead of a dead topic."""
    assert "ENQUEUED" in tel.docs["events_emitted_total"]
    assert "not necessarily delivered" in tel.docs["events_emitted_total"]
    assert "events_delivered_total" in tel.docs["events_emitted_total"]
    assert tel.docs["events_delivered_total"] == "Messages the Kafka broker acknowledged"


def test_acknowledged_messages_are_counted_as_delivered(tm, kafka, tel):
    tm.produce_events_to_kafka(events(4))
    assert tel.value("events_emitted_total") == 4        # enqueued
    assert tel.value("events_delivered_total") == 4      # acknowledged by flush()'s callbacks
    assert tel.value("kafka_delivery_errors_total", reason="enqueue_error") == 0


def test_delivery_failures_are_counted_by_reason_and_logged(tm, kafka, tel, caplog):
    """A missing topic: produce() accepts everything, the broker refuses everything."""
    caplog.set_level(logging.DEBUG)
    kafka.delivery_error = FakeKafkaError("UNKNOWN_TOPIC_OR_PART")
    tm.produce_events_to_kafka(events(10))
    assert tel.value("events_emitted_total") == 10                   # enqueued ...
    assert tel.value("events_delivered_total") == 0                  # ... but never delivered
    assert tel.value("kafka_delivery_errors_total", reason="UNKNOWN_TOPIC_OR_PART") == 10
    lines = [r for r in caplog.records if r.getMessage() == "kafka_delivery_failed"]
    assert len(lines) == 1                                           # rate limited, see below
    (line,) = lines
    assert line.levelno == logging.ERROR
    assert line.topic == "nashville.ticketmaster.events"
    assert line.reason == "UNKNOWN_TOPIC_OR_PART"
    assert "UNKNOWN_TOPIC_OR_PART" in line.error
    assert line.partition == -1 and line.suppressed == 0


def test_different_reasons_are_counted_separately(tm, kafka, tel):
    kafka.delivery_error = FakeKafkaError("TOPIC_AUTHORIZATION_FAILED")
    tm.produce_events_to_kafka(events(3))
    kafka.delivery_error = FakeKafkaError("_MSG_TIMED_OUT")
    tm.produce_events_to_kafka(events(2))
    assert tel.value("kafka_delivery_errors_total", reason="TOPIC_AUTHORIZATION_FAILED") == 3
    assert tel.value("kafka_delivery_errors_total", reason="_MSG_TIMED_OUT") == 2


# ----- log rate limiting ------------------------------------------------------------


def test_the_error_log_is_limited_to_one_line_per_reason_per_minute(tm, tel, caplog, clock):
    caplog.set_level(logging.DEBUG)
    err = FakeKafkaError("TOPIC_AUTHORIZATION_FAILED")
    msg = FakeMessage("nashville.ticketmaster.events", partition=0)

    def lines():
        return [r for r in caplog.records if r.getMessage() == "kafka_delivery_failed"]

    for _ in range(500):                                  # a dead topic, 500 messages in a burst
        tm._on_delivery(err, msg)
    assert len(lines()) == 1 and lines()[0].suppressed == 0
    assert tel.value("kafka_delivery_errors_total", reason="TOPIC_AUTHORIZATION_FAILED") == 500   # counter is exact

    clock.advance(59)
    tm._on_delivery(err, msg)
    assert len(lines()) == 1                              # still inside the 60 s window

    clock.advance(2)                                      # 61 s after the first line
    tm._on_delivery(err, msg)
    assert len(lines()) == 2
    assert lines()[1].suppressed == 500                   # 499 from the burst + 1 at +59 s

    # the suppressed count starts again from zero after every line it is reported on:
    # 3 more messages inside the new window, then the next line says 3, not 503
    for _ in range(3):
        tm._on_delivery(err, msg)
    assert len(lines()) == 2
    clock.advance(61)
    tm._on_delivery(err, msg)
    assert len(lines()) == 3 and lines()[2].suppressed == 3

    # an unrelated reason is not suppressed by the first one's window
    tm._on_delivery(FakeKafkaError("_TRANSPORT"), msg)
    assert len(lines()) == 4 and lines()[3].reason == "_TRANSPORT" and lines()[3].suppressed == 0


def test_the_rate_limit_state_is_only_touched_under_its_lock(tm):
    """The delivery callback runs on whichever thread polls, so the per-reason state is
    guarded by a lock. A race on it cannot be provoked reliably under the GIL, so this
    pins the guard directly: the lock is taken for every failure it covers."""
    class CountingLock:
        taken = 0

        def __enter__(self):
            self.taken += 1

        def __exit__(self, *exc):
            return False

    tm._kafka_log_lock = CountingLock()
    tm._on_delivery(FakeKafkaError("_TRANSPORT"), FakeMessage("t"))
    tm._on_delivery(FakeKafkaError("_TRANSPORT"), FakeMessage("t"))
    assert tm._kafka_log_lock.taken == 2


# ----- enqueue failures ------------------------------------------------------------


def test_an_enqueue_failure_is_counted_logged_and_the_batch_carries_on(tm, kafka, tel, caplog):
    caplog.set_level(logging.DEBUG)
    kafka.enqueue_error = RuntimeError("produce failed for topic=x: Local: Queue full")
    tm.produce_events_to_kafka(events(5))                # must not raise
    assert kafka.produced == []
    assert tel.value("kafka_delivery_errors_total", reason="enqueue_error") == 5
    assert tel.value("events_emitted_total") == 0        # nothing was enqueued
    (line,) = [r for r in caplog.records if r.getMessage() == "kafka_delivery_failed"]
    assert line.reason == "enqueue_error" and "Queue full" in line.error and line.partition is None
    done = [r for r in caplog.records if r.getMessage() == "kafka_batch_enqueued"]
    assert done[0].enqueued == 0 and done[0].enqueue_failed == 5


def test_one_bad_enqueue_does_not_stop_the_rest(tm, kafka, tel):
    real_produce = kafka.produce
    calls = {"n": 0}

    def flaky(topic, value, **kw):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("one bad apple")
        return real_produce(topic, value, **kw)

    kafka.produce = flaky
    tm.produce_events_to_kafka(events(4))
    assert len(kafka.produced) == 3
    assert tel.value("events_emitted_total") == 3
    assert tel.value("kafka_delivery_errors_total", reason="enqueue_error") == 1


# ----- flush / poll ------------------------------------------------------------------


def test_every_batch_ends_with_a_flush(tm, kafka):
    tm.produce_events_to_kafka(events(3))
    assert kafka.flushes == [t.KAFKA_FLUSH_TIMEOUT_S]


def test_an_incomplete_flush_warns(tm, kafka, caplog):
    caplog.set_level(logging.DEBUG)
    kafka.flush_remaining = 7
    tm.produce_events_to_kafka(events(7))
    (warn,) = [r for r in caplog.records if r.getMessage() == "kafka_flush_incomplete"]
    assert warn.levelno == logging.WARNING and warn.queued == 7


def test_a_complete_flush_does_not_warn(tm, kafka, caplog):
    caplog.set_level(logging.DEBUG)
    tm.produce_events_to_kafka(events(3))
    assert not [r for r in caplog.records if r.getMessage() == "kafka_flush_incomplete"]


def test_waiting_serves_delivery_callbacks_every_second(tm, kafka, clock, monkeypatch):
    """The service sleeps an hour between batches; a late acknowledgement or failure
    (e.g. _MSG_TIMED_OUT five minutes after an enqueue) is only reported when polled."""
    tm.poll_interval_seconds = 10
    before = kafka.polls
    tm.wait()
    assert kafka.polls - before == 10
    assert clock.sleeps == [1.0] * 10 and sum(clock.sleeps) == 10


def test_waiting_stops_on_sigterm(tm, kafka, clock, monkeypatch):
    tm.poll_interval_seconds = 3600

    def sleep_then_sigterm(seconds):
        monkeypatch.setattr(t, "_shutdown", True)

    monkeypatch.setattr(t, "_sleep_responsively", sleep_then_sigterm)
    tm.wait()
    assert kafka.polls == 1


# ----- a producer in a fatal state ---------------------------------------------------


def test_a_poll_that_raises_is_counted_and_does_not_kill_the_wait(tm, kafka, tel, clock, caplog):
    """librdkafka raises from poll() once the producer has had a fatal error (e.g. no
    cluster-level permission for idempotent writes). The worker must survive it: it
    would take the independent database sink down with it."""
    caplog.set_level(logging.DEBUG)
    tm.poll_interval_seconds = 5

    def fatal_poll(timeout=0.0):
        raise RuntimeError('KafkaError{FATAL,code=CLUSTER_AUTHORIZATION_FAILED,val=31}')

    kafka.poll = fatal_poll
    tm.wait()                                                  # returns normally after 5 s
    assert clock.sleeps == [1.0] * 5
    assert tel.value("kafka_delivery_errors_total", reason="poll_error") == 5
    lines = [r for r in caplog.records if r.getMessage() == "kafka_delivery_failed"]
    assert len(lines) == 1 and lines[0].reason == "poll_error" and lines[0].suppressed == 0


def test_a_flush_that_raises_is_counted_and_the_batch_still_completes(tm, kafka, tel, caplog):
    caplog.set_level(logging.DEBUG)

    def fatal_flush(timeout=10.0):
        raise RuntimeError("fatal")

    kafka.flush = fatal_flush
    tm.produce_events_to_kafka(events(3))                      # must not raise
    assert tel.value("kafka_delivery_errors_total", reason="flush_error") == 1
    assert [r for r in caplog.records if r.getMessage() == "kafka_batch_enqueued"]
    assert not [r for r in caplog.records if r.getMessage() == "kafka_flush_incomplete"]


# ----- the callback itself -----------------------------------------------------------


def test_the_callback_never_raises(tm, tel, caplog):
    caplog.set_level(logging.DEBUG)

    class Hostile:
        def name(self):
            raise RuntimeError("no name for you")

        def __str__(self):
            raise RuntimeError("no str either")

    tm._on_delivery(Hostile(), object())          # err.name() raises
    tm._on_delivery(FakeKafkaError(), None)       # msg is None: the partition lookup fails ...
    # ... and the failure is STILL counted and logged, with no partition: a message whose
    # partition cannot be read is a failed message all the same.
    assert tel.value("kafka_delivery_errors_total", reason="UNKNOWN_TOPIC_OR_PART") == 1
    (line,) = [r for r in caplog.records if r.getMessage() == "kafka_delivery_failed"]
    assert line.reason == "UNKNOWN_TOPIC_OR_PART" and line.partition is None
    tm._on_delivery(None, None)                   # success
    assert tel.value("events_delivered_total") == 1
    tm._delivery_errors_total = None              # even a broken counter must not escape
    tm._on_delivery(FakeKafkaError(), FakeMessage("t"))


def test_the_callback_is_thread_safe(tm, tel):
    err = FakeKafkaError("_TRANSPORT")

    def hammer():
        for _ in range(2000):
            tm._on_delivery(err, FakeMessage("t"))
            tm._on_delivery(None, FakeMessage("t"))

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert tel.value("kafka_delivery_errors_total", reason="_TRANSPORT") == 8000
    assert tel.value("events_delivered_total") == 8000
