"""Shared fakes for the ticketmaster2kafka tests.

Nothing here needs Docker, Kafka, Postgres or the internet. The service talks to the
outside world through four seams, and each has a fake:

    tel     FakeTel      counters/gauges that remember their values; loggers that either
                         propagate to pytest's caplog or write real JSON to a buffer
    kafka   FakeKafka    records produce() calls, runs the delivery callbacks at
                         poll()/flush() the way librdkafka does, can fail on demand
    db      FakeDb       records the rows insert_events() was given, all-or-nothing per
                         call like the real transaction, can refuse rows on demand
    HTTP    FakeRequests / FakeServer
                         requests.get replaced by a script, or a real loopback HTTP
                         server on 127.0.0.1 (ephemeral port) when real `requests`
                         behaviour matters (URLs inside exception text, paging params)

The suite is hermetic: the `hermetic_environment` fixture below clears every proxy
variable (a proxy in the shell must not intercept the loopback fakes) and refuses any
socket connection that is not to loopback, so a test that reached for the real
network fails loudly instead of quietly depending on it.

The service is a flat script, not an installed package: putting the repo root on
sys.path here is what makes `import ticketmaster2kafka` resolve from tests/.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
import socket
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT)]

import ticketmaster2kafka as t  # noqa: E402

URLLIB3_LEVEL_AT_START = logging.getLogger("urllib3").level   # before any test touches it
FAKE_KEY = "TESTKEY0123456789abcdefSECRETVALUE"
UTC = dt.timezone.utc


# ----- telemetry -----------------------------------------------------------------


class FakeChild:
    def __init__(self, metric, key):
        self._metric, self._key = metric, key
        metric.values.setdefault(key, 0)

    def inc(self, n=1):
        self._metric.values[self._key] += n

    def set(self, v):
        self._metric.values[self._key] = v


class FakeMetric:
    """Counter / gauge / histogram in one: just remembers what was done to it."""

    def __init__(self):
        self.values = {(): 0}

    def labels(self, **labels):
        return FakeChild(self, tuple(sorted(labels.items())))

    def inc(self, n=1):
        self.values[()] += n

    def set(self, v):
        self.values[()] = v

    def observe(self, v):
        self.values[()] += 1

    @contextmanager
    def time(self):
        yield
        self.values[()] += 1


class FakeTel:
    """Stands in for lv_telemetry_connector's Telemetry.

    Loggers are plain stdlib loggers under `test.` (so pytest's caplog sees them), unless
    `json_stream` is given: then they are the real lv_telemetry_connector JSON loggers
    writing to that buffer, which is what the key-leak tests need.
    """

    _n = 0

    def __init__(self, json_stream=None):
        self.metrics: dict[str, FakeMetric] = {}
        self.docs: dict[str, str] = {}          # metric name -> its help text
        self._tel = None
        if json_stream is not None:
            from lv_telemetry_connector import Telemetry
            FakeTel._n += 1
            self._tel = Telemetry(namespace=f"tmtest{FakeTel._n}")
            self._tel.configure_logging(level=logging.DEBUG, stream=json_stream,
                                        service="ticketmaster2kafka")

    def get_logger(self, name):
        if self._tel is not None:
            return self._tel.get_logger(name)
        return logging.getLogger(f"test.{name}")

    def _metric(self, name, labelnames=(), doc=""):
        self.docs[name] = doc
        m = self.metrics.setdefault(name, FakeMetric())
        if labelnames:
            m.values.pop((), None)     # a labelled metric has no unlabelled sample
        return m

    def counter(self, name, doc, labelnames=()):
        return self._metric(name, labelnames, doc)

    def gauge(self, name, doc, labelnames=()):
        return self._metric(name, labelnames, doc)

    def histogram(self, name, doc, labelnames=(), buckets=None):
        return self._metric(name, labelnames, doc)

    def value(self, name, **labels):
        return self.metrics[name].values.get(tuple(sorted(labels.items())), 0)


# ----- Kafka ---------------------------------------------------------------------


class FakeKafkaError:
    def __init__(self, name="UNKNOWN_TOPIC_OR_PART", text="Broker: Unknown topic or partition"):
        self._name, self._text = name, text

    def name(self):
        return self._name

    def __str__(self):
        return f'KafkaError{{code={self._name},val=3,str="{self._text}"}}'


class FakeMessage:
    def __init__(self, topic, partition=-1):
        self._topic, self._partition = topic, partition

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition


class FakeKafka:
    """Mimics lv_kafka_connector's KafkaProducer as the service uses it.

    produce() really does swallow enqueue errors unless raise_on_error=True (that is
    the connector's behaviour and the reason the service must pass it). Delivery
    callbacks run at poll() and flush(), like librdkafka's.
    """

    def __init__(self):
        self.produced: list[dict] = []
        self.polls = 0
        self.flushes: list[float] = []
        self.flush_remaining = 0
        self.delivery_error: FakeKafkaError | None = None   # None = broker acks everything
        self.enqueue_error: Exception | None = None          # raised from produce() when set
        self._undelivered: list[dict] = []

    def produce(self, topic, value, *, key=None, headers=None, partition=None,
                on_delivery=None, raise_on_error=False):
        if self.enqueue_error is not None:
            if raise_on_error:
                raise self.enqueue_error
            return                                           # swallowed, as the connector does
        msg = dict(topic=topic, value=value, key=key, headers=headers, partition=partition,
                   on_delivery=on_delivery, raise_on_error=raise_on_error)
        self.produced.append(msg)
        self._undelivered.append(msg)

    def _serve_callbacks(self):
        pending, self._undelivered = self._undelivered, []
        for m in pending:
            if m["on_delivery"] is not None:
                m["on_delivery"](self.delivery_error, FakeMessage(m["topic"]))
        return len(pending)

    def poll(self, timeout=0.0):
        self.polls += 1
        return self._serve_callbacks()

    def flush(self, timeout=10.0):
        self.flushes.append(timeout)
        self._serve_callbacks()
        return self.flush_remaining


# ----- database ------------------------------------------------------------------


class FakeDb:
    """All-or-nothing per call, like the real transaction: a call that raises stores
    nothing, a call that succeeds stores every row it was given in `stored`."""

    def __init__(self):
        self.calls: list[list[dict]] = []       # the rows of every call, failed or not
        self.stored: list[dict] = []            # rows really written (successful calls)
        self.failures: list[Exception] = []     # raised, one per call, until exhausted
        self.refuse = None                      # optional fn(rows) -> Exception | None

    def insert_events(self, rows):
        self.calls.append(rows)
        if self.failures:
            raise self.failures.pop(0)
        if self.refuse is not None:
            err = self.refuse(rows)
            if err is not None:
                raise err
        self.stored.extend(rows)


def pg_error(sqlstate, message="database said no"):
    """What the connector raises for a failed insert: an LvDbQueryError raised `from` the
    psycopg error, and it is the psycopg error that carries the SQLSTATE."""
    from lv_db_connector.exceptions import LvDbQueryError
    cause = Exception(message)
    cause.sqlstate = sqlstate
    err = LvDbQueryError(message)
    err.__cause__ = cause
    return err


# ----- HTTP: scripted requests.get ----------------------------------------------


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("Expecting value: line 1 column 1 (char 0)")
        return self._body


class FakeRequests:
    """Replacement for requests.get. `script` is a list of FakeResponse or exceptions,
    consumed one per call (the last one repeats forever)."""

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[dict] = []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        item = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(item, Exception):
            raise item
        return item


def page_doc(events, has_next=False):
    doc = {"_links": {"self": {"href": "x"}}}
    if has_next:
        doc["_links"]["next"] = {"href": "x"}
    if events:
        doc["_embedded"] = {"events": events}
    return doc


# ----- HTTP: a real loopback server ----------------------------------------------


class FakeServer:
    """A real HTTP server on 127.0.0.1 (ephemeral port) imitating the Discovery API.

    mode: "ok" serves `events` page by page; "500", "403", "429" answer with that status;
    "drop" closes the connection without answering; "badjson" answers 200 with HTML.
    """

    def __init__(self):
        self.mode = "ok"
        self.events: list[dict] = []
        self.requests: list[str] = []     # raw request targets, query string included
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                outer.requests.append(self.path)
                mode = outer.mode
                if mode == "drop":
                    self.connection.close()
                    return
                if mode in ("500", "403", "429"):
                    self._send(int(mode), {"fault": {"faultstring": "nope"}})
                    return
                if mode == "badjson":
                    body = b"<html>proxy error</html>"
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                q = {k: v[0] for k, v in parse_qs(urlsplit(self.path).query).items()}
                size, page = int(q.get("size", 20)), int(q.get("page", 0))
                window = outer.events[size * page: size * (page + 1)]
                self._send(200, page_doc(window, has_next=size * (page + 1) < len(outer.events)))

            def _send(self, code, doc):
                body = json.dumps(doc).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._httpd.server_address[1]}"
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()


# ----- fake clock ----------------------------------------------------------------


class FakeClock:
    """One clock for everything the service reads: wall time (the query window),
    monotonic time (rate limits, wait()) and sleeping (which just advances both)."""

    def __init__(self, start):
        self.now = start
        self.mono = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self):
        return self.mono

    def advance(self, seconds):
        self.now += dt.timedelta(seconds=seconds)
        self.mono += seconds

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


# ----- events --------------------------------------------------------------------


def make_event(eid="Z7r9jZ0001", name="Fixture Event", start="2026-11-01T01:00:00Z", **over):
    ev = {
        "name": name,
        "type": "event",
        "id": eid,
        "test": False,
        "url": f"https://www.ticketmaster.com/event/{eid}",
        "locale": "en-us",
        "images": [{"ratio": "16_9", "url": "https://s1.ticketm.net/a.jpg", "width": 640}],
        "sales": {"public": {"startDateTime": "2026-08-01T15:00:00Z", "endDateTime": start}},
        "dates": {"start": {"localDate": "2026-10-31", "localTime": "20:00:00", "dateTime": start},
                  "timezone": "America/Chicago", "status": {"code": "onsale"}},
        "classifications": [{"primary": True, "segment": {"name": "Music"}, "genre": {"name": "Rock"}}],
        "priceRanges": [{"type": "standard", "currency": "USD", "min": 49.5, "max": 199}],
        "_embedded": {
            "venues": [{"id": "KovZpZAEkvaA", "name": "Bridgestone Arena", "postalCode": "37201",
                        "timezone": "America/Chicago", "city": {"name": "Nashville"},
                        "state": {"stateCode": "TN"}, "country": {"countryCode": "US"},
                        "address": {"line1": "501 Broadway"},
                        "location": {"longitude": "-86.778", "latitude": "36.159"}}],
            "attractions": [{"name": "The Band"}],
        },
    }
    ev.update(over)
    return ev


# ----- fixtures ------------------------------------------------------------------


PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                   "http_proxy", "https_proxy", "all_proxy")


def clear_proxy_env(monkeypatch):
    """No proxy for anything the tests talk to. The real-`requests` tests go to a fake on
    127.0.0.1, and a proxy variable in the developer's or CI's shell would send those
    requests to the proxy instead. NO_PROXY is set as well (both spellings: requests
    reads the lower-case one first) so that even the operating system's own proxy
    settings, which requests falls back to on macOS, cannot take loopback."""
    for name in PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")


def is_loopback(host):
    return host in (None, "", "localhost", "::1") or str(host).startswith("127.")


@pytest.fixture(autouse=True)
def hermetic_environment(monkeypatch):
    """Proxies cleared, every non-loopback connection refused, and urllib3's logger level
    (the service silences it, and the leak tests open it) put back after each test."""
    clear_proxy_env(monkeypatch)

    real_connect, real_connect_ex, real_getaddrinfo = (
        socket.socket.connect, socket.socket.connect_ex, socket.getaddrinfo)

    def refuse(host):
        raise OSError(f"blocked by tests/conftest.py: only loopback connections are allowed, got {host!r}")

    def connect(self, address):
        if isinstance(address, tuple) and not is_loopback(address[0]):
            refuse(address[0])
        return real_connect(self, address)

    def connect_ex(self, address):
        if isinstance(address, tuple) and not is_loopback(address[0]):
            refuse(address[0])
        return real_connect_ex(self, address)

    def getaddrinfo(host, *args, **kwargs):
        if not is_loopback(host):
            refuse(host)
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)

    urllib3_level = logging.getLogger("urllib3").level
    yield
    logging.getLogger("urllib3").setLevel(urllib3_level)


@pytest.fixture(autouse=True)
def clean_service_state(monkeypatch):
    """Every test starts from the service's defaults: a fake key, no TM_* overrides, not
    shutting down, and no real sleeping (tests that care replace _sleep_responsively)."""
    monkeypatch.setenv("TICKETMASTER_API_KEY", FAKE_KEY)
    for name in ("TM_START_ISO", "TM_END_ISO", "TM_EXTRA_PARAMS", "TM_WINDOW_DAYS",
                 "TM_PAGE_SIZE", "TM_COUNTRY_CODE", "KAFKA_TOPIC_BASENAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(t, "_shutdown", False)
    monkeypatch.setattr(t, "_worker_failed", False)
    monkeypatch.setattr(t, "_sleep_responsively", lambda seconds: None)


@pytest.fixture
def tel():
    return FakeTel()


@pytest.fixture
def kafka():
    return FakeKafka()


@pytest.fixture
def db():
    return FakeDb()


@pytest.fixture
def make_producer(tel, kafka, db):
    """Factory: make_producer(**kw) -> TicketmasterEventsProducer wired to the fakes."""
    def make(query_params=None, poll_minutes=60, tel_=None):
        return t.TicketmasterEventsProducer(
            "https://app.ticketmaster.com", poll_minutes, kafka=kafka, db=db,
            tel=tel_ or tel, query_params=query_params)
    return make


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock(dt.datetime(2026, 10, 4, 12, 0, 0, tzinfo=UTC))

    class FrozenDatetime(dt.datetime):
        """The service's `datetime` name, whose now() reads the fake clock."""

        @classmethod
        def now(cls, tz=None):
            return c.now.astimezone(tz) if tz else c.now.replace(tzinfo=None)

    monkeypatch.setattr(t, "datetime", FrozenDatetime)
    monkeypatch.setattr(t, "_sleep_responsively", c.sleep)
    monkeypatch.setattr(t.time, "monotonic", c.monotonic)
    return c


@pytest.fixture
def fake_server():
    s = FakeServer()
    yield s
    s.close()


@pytest.fixture
def json_log():
    """A buffer the real JSON loggers write to; .lines() parses it."""
    class Buf(io.StringIO):
        def lines(self):
            return [json.loads(l) for l in self.getvalue().splitlines() if l.startswith("{")]
    return Buf()
