"""Upstream fetch failures: classification, counters, bounded retry, shutdown (TM-14/15, TM-08).

The old code retried HTTP 429 forever every 2 s, ignoring SIGTERM, and treated every
other hiccup as "lose the whole hourly snapshot". These tests pin the replacement:
at most FETCH_MAX_ATTEMPTS tries per page for 429/5xx/timeouts/connection errors, with
exponential backoff and jitter; no retry for a bad key or an unreadable body; every
failed attempt counted and logged; _shutdown honoured between attempts and pages.
"""

from __future__ import annotations

import logging

import pytest
import requests

import ticketmaster2kafka as t
from conftest import FakeRequests, FakeResponse, make_event, page_doc


@pytest.fixture
def tm(make_producer):
    return make_producer(dict(t.DEFAULT_QUERY_PARAMS))


def failed_lines(caplog):
    return [r for r in caplog.records if r.getMessage() == "upstream_fetch_failed"]


# ----- the happy path ------------------------------------------------------------


def test_pages_are_followed_and_every_event_is_returned(monkeypatch, tm, tel):
    pages = [FakeResponse(200, page_doc([make_event("a"), make_event("b")], has_next=True)),
             FakeResponse(200, page_doc([make_event("c")]))]
    monkeypatch.setattr(t.requests, "get", FakeRequests(pages))
    events = tm._fetch_events()
    assert [e["id"] for e in events] == ["a", "b", "c"]
    calls = t.requests.get.calls
    assert [c["params"]["page"] for c in calls] == [0, 1]
    assert all(c["params"]["apikey"] == "TESTKEY0123456789abcdefSECRETVALUE" for c in calls)
    assert all(c["timeout"] == t.HTTP_TIMEOUT_S for c in calls)
    assert tel.value("events_fetched_total") == 3
    assert tel.value("last_fetch_success_timestamp_seconds") > 0
    assert sum(v for v in tel.metrics["fetch_errors_total"].values.values()) == 0


def test_the_fetch_error_series_exist_at_zero_before_any_failure(tm, tel):
    assert {dict(k)["reason"] for k in tel.metrics["fetch_errors_total"].values} == {
        "timeout", "connection", "http_429", "http_4xx", "http_5xx", "invalid_response"}


def test_deep_paging_stops_at_1000_results(monkeypatch, tm):
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()], has_next=True))]))
    tm._fetch_events()
    assert len(t.requests.get.calls) == 5      # pages 0..4 of 200 = 1000


# ----- 429 (TM-14) ---------------------------------------------------------------


def test_429_is_retried_at_most_three_times_then_the_cycle_fails_cleanly(monkeypatch, tm, tel, caplog):
    caplog.set_level(logging.DEBUG)
    sleeps = []
    monkeypatch.setattr(t, "_sleep_responsively", sleeps.append)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(429, {"fault": {}})]))

    assert tm._fetch_events() is None                      # fails cleanly, no exception

    assert len(t.requests.get.calls) == t.FETCH_MAX_ATTEMPTS == 3
    assert len(sleeps) == 2                                # no sleep after the last attempt
    assert tel.value("fetch_errors_total", reason="http_429") == 3
    assert tel.value("events_fetched_total") == 0
    assert tel.value("last_fetch_success_timestamp_seconds") == 0
    lines = failed_lines(caplog)
    assert [l.attempt for l in lines] == [1, 2, 3]
    assert all(l.levelno == logging.ERROR and l.status_code == 429 and l.reason == "http_429"
               and l.upstream == "app.ticketmaster.com" and l.path == "/discovery/v2/events.json"
               for l in lines)
    assert [l.getMessage() for l in caplog.records if l.levelno == logging.WARNING] == ["fetch_cycle_abandoned"]


def test_backoff_grows_exponentially_and_is_jittered(monkeypatch, tm):
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(503)]))
    runs = []
    for _ in range(40):
        sleeps = []
        monkeypatch.setattr(t, "_sleep_responsively", sleeps.append)
        tm._fetch_events()
        runs.append(sleeps)
    first = [r[0] for r in runs]
    second = [r[1] for r in runs]
    base = t.FETCH_BACKOFF_BASE_S
    assert all(0.5 * base <= s <= 1.5 * base for s in first)
    assert all(0.5 * 2 * base <= s <= 1.5 * 2 * base for s in second)
    assert len(set(first)) > 1                              # jitter: not one fixed value


def test_shutdown_between_attempts_stops_retrying(monkeypatch, tm):
    def sleep_then_sigterm(seconds):
        monkeypatch.setattr(t, "_shutdown", True)

    monkeypatch.setattr(t, "_sleep_responsively", sleep_then_sigterm)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(429)]))
    assert tm._fetch_events() is None
    assert len(t.requests.get.calls) == 1                   # not 3, and certainly not forever


def test_shutdown_between_pages_stops_paging(monkeypatch, tm):
    class ShutdownAfterFirstPage(FakeRequests):
        def __call__(self, *a, **k):
            r = super().__call__(*a, **k)
            monkeypatch.setattr(t, "_shutdown", True)
            return r

    monkeypatch.setattr(t.requests, "get", ShutdownAfterFirstPage(
        [FakeResponse(200, page_doc([make_event()], has_next=True))]))
    assert tm._fetch_events() is None
    assert len(t.requests.get.calls) == 1


def test_shutdown_already_requested_makes_no_request(monkeypatch, tm):
    monkeypatch.setattr(t, "_shutdown", True)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([]))]))
    assert tm._fetch_events() is None
    assert t.requests.get.calls == []


# ----- other failures (TM-15) ----------------------------------------------------


def test_one_503_is_retried_and_the_snapshot_is_not_lost(monkeypatch, tm, tel):
    monkeypatch.setattr(t.requests, "get", FakeRequests(
        [FakeResponse(503), FakeResponse(200, page_doc([make_event("a")]))]))
    events = tm._fetch_events()
    assert [e["id"] for e in events] == ["a"]
    assert tel.value("fetch_errors_total", reason="http_5xx") == 1
    assert tel.value("events_fetched_total") == 1
    assert tel.value("last_fetch_success_timestamp_seconds") > 0


@pytest.mark.parametrize("exc, reason, error_class", [
    (requests.exceptions.ConnectTimeout("x"), "timeout", "ConnectTimeout"),
    (requests.exceptions.ReadTimeout("x"), "timeout", "ReadTimeout"),
    (requests.exceptions.ConnectionError("x"), "connection", "ConnectionError"),
    (requests.exceptions.SSLError("x"), "connection", "SSLError"),
    (requests.exceptions.ChunkedEncodingError("x"), "connection", "ChunkedEncodingError"),
])
def test_exceptions_are_classified_and_retried(monkeypatch, tm, tel, caplog, exc, reason, error_class):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(t.requests, "get", FakeRequests([exc]))
    assert tm._fetch_events() is None
    assert len(t.requests.get.calls) == 3
    assert tel.value("fetch_errors_total", reason=reason) == 3
    lines = failed_lines(caplog)
    assert len(lines) == 3
    assert all(l.reason == reason and l.error_class == error_class and l.status_code is None
               for l in lines)


@pytest.mark.parametrize("status, reason", [
    (401, "http_4xx"), (403, "http_4xx"), (404, "http_4xx"), (400, "http_4xx")])
def test_client_errors_are_not_retried(monkeypatch, tm, tel, caplog, status, reason):
    caplog.set_level(logging.DEBUG)
    sleeps = []
    monkeypatch.setattr(t, "_sleep_responsively", sleeps.append)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(status)]))
    assert tm._fetch_events() is None
    assert len(t.requests.get.calls) == 1 and sleeps == []
    assert tel.value("fetch_errors_total", reason=reason) == 1
    (line,) = failed_lines(caplog)
    assert line.status_code == status and line.attempt == 1


@pytest.mark.parametrize("response", [
    FakeResponse(200, None),                                   # not JSON
    FakeResponse(200, ["not", "a", "dict"]),                   # JSON, wrong shape
    FakeResponse(200, {"_embedded": {"events": "nope"}}),      # events not a list
])
def test_an_unreadable_200_is_counted_invalid_response_and_not_retried(monkeypatch, tm, tel, response):
    monkeypatch.setattr(t.requests, "get", FakeRequests([response]))
    assert tm._fetch_events() is None
    assert len(t.requests.get.calls) == 1
    assert tel.value("fetch_errors_total", reason="invalid_response") == 1


def test_a_failure_on_a_later_page_discards_the_partial_result(monkeypatch, tm, tel):
    monkeypatch.setattr(t.requests, "get", FakeRequests(
        [FakeResponse(200, page_doc([make_event("a")], has_next=True)), FakeResponse(500)]))
    assert tm._fetch_events() is None
    assert tel.value("events_fetched_total") == 0              # no half snapshot


def test_the_hourly_loop_survives_a_failed_cycle(monkeypatch, tel, kafka, db, clock):
    monkeypatch.setattr(t.requests, "get", FakeRequests(
        [FakeResponse(500), FakeResponse(500), FakeResponse(500),
         FakeResponse(200, page_doc([make_event("a")]))]))
    cycles = {"n": 0}

    def fake_wait(self):
        cycles["n"] += 1
        if cycles["n"] == 2:
            monkeypatch.setattr(t, "_shutdown", True)

    monkeypatch.setattr(t.TicketmasterEventsProducer, "wait", fake_wait)
    t.update_ticketmaster_events("https://app.ticketmaster.com", 60, kafka, db, tel,
                                 dict(t.DEFAULT_QUERY_PARAMS))
    assert cycles["n"] == 2
    assert len(kafka.produced) == 1 and len(db.calls) == 1     # cycle 2 delivered
    assert tel.value("fetch_errors_total", reason="http_5xx") == 3


# ----- real requests, real sockets (loopback) ------------------------------------


def test_against_a_real_http_server_the_window_and_paging_go_over_the_wire(monkeypatch, clock, fake_server, kafka, db, tel):
    fake_server.events = [make_event(f"e{i}") for i in range(450)]
    monkeypatch.setenv("TM_PAGE_SIZE", "200")
    tm = t.TicketmasterEventsProducer(fake_server.url, 60, kafka=kafka, db=db, tel=tel,
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    events = tm._fetch_events()
    assert len(events) == 450
    assert len(fake_server.requests) == 3
    first = fake_server.requests[0]
    assert "startDateTime=2026-10-04T12%3A00%3A00Z" in first and "endDateTime=2026-11-01T12%3A00%3A00Z" in first
    assert "geoPoint=dn6m9qgn" in first and "unit=miles" in first and "units=" not in first and "countryCode=US" in first
    assert "apikey=" in first and "size=200" in first and "page=0" in first


@pytest.mark.parametrize("mode, reason", [
    ("500", "http_5xx"), ("403", "http_4xx"), ("429", "http_429"),
    ("drop", "connection"), ("badjson", "invalid_response")])
def test_real_http_failure_modes_are_classified(monkeypatch, fake_server, kafka, db, tel, mode, reason):
    fake_server.mode = mode
    tm = t.TicketmasterEventsProducer(fake_server.url, 60, kafka=kafka, db=db, tel=tel,
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    assert tm._fetch_events() is None
    assert tel.value("fetch_errors_total", reason=reason) >= 1
