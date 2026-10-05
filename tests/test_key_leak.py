"""The API key must never reach a log line (TM-07).

The key travels as `?apikey=...` in the request URL, and the text of every requests
exception embeds that URL. The old code logged `log.exception(e, exc_info=True)`, so any
4xx/5xx/connect failure wrote the key to the JSON logs (and so to Loki), twice per line.

Every test here uses the REAL JSON loggers (lv_telemetry_connector) writing to a buffer,
with the root logger opened to DEBUG and a catch-all handler attached so that third-party
loggers (urllib3) are captured too, and then asserts the key string appears nowhere in
anything that was logged, at any level.
"""

from __future__ import annotations

import io
import logging

import pytest
import requests

import ticketmaster2kafka as t
from conftest import (FAKE_KEY, URLLIB3_LEVEL_AT_START, FakeRequests, FakeResponse, FakeTel,
                      make_event, page_doc)


@pytest.fixture
def everything_logged(json_log):
    """Capture ALL logging from ALL loggers, as text, at DEBUG."""
    root = logging.getLogger()
    third_party = io.StringIO()
    handler = logging.StreamHandler(third_party)
    old_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    # What the process looks like when somebody has turned urllib3's logging on:
    logging.getLogger("urllib3").setLevel(logging.NOTSET)

    class Everything:
        def text(self):
            return json_log.getvalue() + third_party.getvalue()

    yield Everything()
    root.removeHandler(handler)
    root.setLevel(old_level)


def url_with_key(extra=""):
    return f"https://app.ticketmaster.com/discovery/v2/events.json?geoPoint=dn6m9qgn&apikey={FAKE_KEY}&size=200&page=0{extra}"


# What requests/urllib3 really put in the message of each exception, key included.
LEAKY_EXCEPTIONS = [
    requests.exceptions.ConnectionError(
        f"HTTPSConnectionPool(host='app.ticketmaster.com', port=443): Max retries exceeded with url: "
        f"/discovery/v2/events.json?apikey={FAKE_KEY}&page=0 (Caused by NewConnectionError('...'))"),
    requests.exceptions.ConnectTimeout(
        f"HTTPSConnectionPool(host='app.ticketmaster.com', port=443): Max retries exceeded with url: "
        f"/discovery/v2/events.json?apikey={FAKE_KEY} (Caused by ConnectTimeoutError(...))"),
    requests.exceptions.HTTPError(f"401 Client Error: Unauthorized for url: {url_with_key()}"),
    requests.exceptions.HTTPError(f"500 Server Error: Internal Server Error for url: {url_with_key()}"),
    requests.exceptions.TooManyRedirects(f"Exceeded 30 redirects. {url_with_key()}"),
    requests.exceptions.InvalidURL(f"Invalid URL {url_with_key()!r}"),
]


@pytest.mark.parametrize("exc", LEAKY_EXCEPTIONS, ids=lambda e: type(e).__name__)
def test_no_request_exception_text_reaches_any_log(monkeypatch, exc, json_log, everything_logged):
    tm = t.TicketmasterEventsProducer("https://app.ticketmaster.com", 60, kafka=object(), db=object(),
                                      tel=FakeTel(json_stream=json_log),
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    monkeypatch.setattr(t.requests, "get", FakeRequests([exc]))
    assert tm._fetch_events() is None
    assert json_log.lines(), "the failure must still be logged"
    assert FAKE_KEY not in everything_logged.text()
    assert "apikey" not in everything_logged.text().lower()


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500, 503])
def test_http_status_failures_log_only_sanitized_fields(monkeypatch, status, json_log, everything_logged):
    tm = t.TicketmasterEventsProducer("https://app.ticketmaster.com", 60, kafka=object(), db=object(),
                                      tel=FakeTel(json_stream=json_log),
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(status)]))
    tm._fetch_events()
    failed = [l for l in json_log.lines() if l["message"] == "upstream_fetch_failed"]
    assert failed
    for line in failed:
        assert line["upstream"] == "app.ticketmaster.com"
        assert line["path"] == "/discovery/v2/events.json"        # no query string
        assert "?" not in line["path"]
        assert line["status_code"] == status
        assert "exc_info" not in line
    assert FAKE_KEY not in everything_logged.text()


def test_a_url_with_credentials_in_it_logs_only_the_host(monkeypatch, json_log, everything_logged):
    tm = t.TicketmasterEventsProducer("https://user:hunter2@app.ticketmaster.com:8443", 60, kafka=object(),
                                      db=object(), tel=FakeTel(json_stream=json_log),
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(500)]))
    tm._fetch_events()
    assert "hunter2" not in everything_logged.text()
    assert all(l["upstream"] == "app.ticketmaster.com" for l in json_log.lines()
               if l["message"] == "upstream_fetch_failed")


def test_real_requests_against_a_dead_port_do_not_leak(json_log, everything_logged):
    """No mocking at all: a real connection refused, with the real exception text."""
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        dead_port = s.getsockname()[1]            # nothing listens here once it is closed
    tm = t.TicketmasterEventsProducer(f"http://127.0.0.1:{dead_port}", 60, kafka=object(), db=object(),
                                      tel=FakeTel(json_stream=json_log),
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    assert tm._fetch_events() is None
    failed = [l for l in json_log.lines() if l["message"] == "upstream_fetch_failed"]
    assert len(failed) == 3 and all(l["reason"] == "connection" for l in failed)
    assert FAKE_KEY not in everything_logged.text()


@pytest.mark.parametrize("mode", ["500", "403", "429", "drop", "badjson", "ok"])
def test_real_http_server_at_debug_never_leaks(mode, fake_server, json_log, everything_logged):
    fake_server.mode = mode
    fake_server.events = [make_event("a")]
    tm = t.TicketmasterEventsProducer(fake_server.url, 60, kafka=object(), db=object(),
                                      tel=FakeTel(json_stream=json_log),
                                      query_params=dict(t.DEFAULT_QUERY_PARAMS))
    tm._fetch_events()
    assert any(FAKE_KEY in req for req in fake_server.requests), "the key was really sent upstream"
    assert FAKE_KEY not in everything_logged.text()


def test_urllib3_is_silenced_so_its_url_lines_cannot_appear(json_log):
    logging.getLogger("urllib3").setLevel(logging.NOTSET)
    t.TicketmasterEventsProducer("https://app.ticketmaster.com", 60, kafka=object(), db=object(),
                                 tel=FakeTel(), query_params={})
    assert logging.getLogger("urllib3").getEffectiveLevel() >= logging.WARNING
    assert not logging.getLogger("urllib3.connectionpool").isEnabledFor(logging.WARNING)


def test_a_tests_urllib3_level_change_does_not_outlive_it():
    """The test above leaves urllib3 at CRITICAL (the producer silences it). The autouse
    hermetic_environment fixture puts the level back, so this next test sees the
    default again. (Order-dependent by nature: it can only fail right after that test.)"""
    assert logging.getLogger("urllib3").level == URLLIB3_LEVEL_AT_START


# ----- the last line of defence: the unexpected-exception handlers -----------------


def test_redacted_traceback_masks_the_query_string_form_and_the_literal_key():
    try:
        raise RuntimeError(f"boom for url: {url_with_key()} and the bare key {FAKE_KEY}")
    except RuntimeError:
        text = t._redacted_traceback()
    assert FAKE_KEY not in text
    assert "apikey=<redacted>" in text
    assert "RuntimeError" in text and "Traceback" in text      # still a useful traceback


@pytest.mark.parametrize("text, secret", [
    ("failed for https://app.ticketmaster.com/x?apikey=ROTATEDKEY0123456789&size=200 (retry)", "ROTATEDKEY0123456789"),
    ("failed for https://app.ticketmaster.com/x?APIKEY=UPPERCASEKEYVALUE99&page=0", "UPPERCASEKEYVALUE99"),
    ("failed for https://app.ticketmaster.com/x?api_key=UNDERSCOREKEY4242&page=0", "UNDERSCOREKEY4242"),
    ("failed for https://app.ticketmaster.com/x?api-key=HYPHENKEY4242&page=0", "HYPHENKEY4242"),
    ("failed for https://app.ticketmaster.com/x?apikey=url%2Fenc%3Doded%2BKEY&page=0", "url%2Fenc%3Doded%2BKEY"),
    ("Invalid URL 'https://app.ticketmaster.com/x?apikey=QUOTEDKEY777'", "QUOTEDKEY777"),
])
def test_redacted_traceback_masks_an_api_key_that_is_not_the_configured_one(text, secret):
    """The apikey=<value> pattern exists for a key that differs from TICKETMASTER_API_KEY
    (a rotated or URL-encoded one). Every value here differs from the configured key, so
    only the pattern, not the literal replacement of the configured key, can mask it."""
    assert secret != FAKE_KEY
    try:
        raise RuntimeError(text)
    except RuntimeError:
        out = t._redacted_traceback()
    assert secret not in out
    assert "<redacted>" in out
    assert "failed for https://app.ticketmaster.com/x?" in out or "Invalid URL" in out   # the rest survives


def test_redacted_traceback_stops_at_the_end_of_the_value_not_the_line():
    try:
        raise RuntimeError("url https://h/x?apikey=SECRETVALUE&size=200&page=3 then more text")
    except RuntimeError:
        out = t._redacted_traceback()
    assert "SECRETVALUE" not in out
    assert "size=200&page=3 then more text" in out


def test_worker_crash_log_is_redacted(json_log, everything_logged):
    log = FakeTel(json_stream=json_log).get_logger("main")

    def worker():
        raise requests.exceptions.HTTPError(f"401 Client Error: Unauthorized for url: {url_with_key()}")

    t.thread_wrapper(worker, name="ticketmaster_events", log=log)()
    (crash,) = [l for l in json_log.lines() if l["message"] == "worker_thread_crashed"]
    assert crash["level"] == "CRITICAL" and crash["error_class"] == "HTTPError"
    assert "apikey=<redacted>" in crash["traceback"]
    assert FAKE_KEY not in everything_logged.text()
    assert t._worker_failed is True and t._shutdown is True


@pytest.mark.parametrize("stage", ["fetch", "kafka", "db"])
def test_loop_level_catch_all_handlers_are_redacted(monkeypatch, stage, json_log, everything_logged, kafka, db):
    """Whatever blows up inside a stage, with the key in its text, is masked."""
    tel = FakeTel(json_stream=json_log)
    leaky = RuntimeError(f"failed for {url_with_key()}")
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event("a")]))]))
    if stage == "fetch":
        monkeypatch.setattr(t.TicketmasterEventsProducer, "_fetch_events",
                            lambda self: (_ for _ in ()).throw(leaky))
    elif stage == "kafka":
        monkeypatch.setattr(t.TicketmasterEventsProducer, "produce_events_to_kafka",
                            lambda self, events: (_ for _ in ()).throw(leaky))
    else:
        monkeypatch.setattr(t.TicketmasterEventsProducer, "insert_events",
                            lambda self, events: (_ for _ in ()).throw(leaky))
    monkeypatch.setattr(t.TicketmasterEventsProducer, "wait",
                        lambda self: monkeypatch.setattr(t, "_shutdown", True))
    t.update_ticketmaster_events("https://app.ticketmaster.com", 60, kafka, db, tel,
                                 dict(t.DEFAULT_QUERY_PARAMS))
    errors = [l for l in json_log.lines() if l["level"] == "ERROR"]
    assert errors and all("exc_info" not in l for l in errors)
    assert FAKE_KEY not in everything_logged.text()
