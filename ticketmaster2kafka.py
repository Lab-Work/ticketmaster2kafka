"""ticketmaster2kafka — Ticketmaster Discovery events to Kafka and Postgres.

Pages the Ticketmaster Discovery API for events in the configured geo window,
produces one message per event to Kafka, and writes a flattened row per event
to the `geo_feeds.ticketmaster_events` TimescaleDB hypertable (see
`sql/ticketmaster_events.sql`).

    Run:        python ticketmaster2kafka.py
    Logs:       JSON on stdout (Loki-friendly)
    Metrics:    /metrics endpoint on :9100 (Prometheus)

Failure visibility (what to alert on; all names are prefixed `ticketmaster2kafka_`):

    fetch_errors_total{reason}            a failed upstream request (timeout, connection,
                                          http_429, http_4xx, http_5xx, invalid_response)
    last_fetch_success_timestamp_seconds  unix time of the last fully fetched cycle
    kafka_delivery_errors_total{reason}   a message Kafka refused or never acknowledged
                                          (reason = the librdkafka error name, or
                                          `enqueue_error` when it never left this process,
                                          or `poll_error` / `flush_error` when the producer
                                          is in a fatal state)
    events_delivered_total                messages the broker acknowledged
    events_emitted_total                  messages ENQUEUED (not necessarily delivered)
    events_invalid_total{reason}          events skipped for the database (missing_id,
                                          missing_name, malformed), or rows the database
                                          itself refused (db_rejected)
    db_write_errors_total                 a failed insert, whatever the cause: a lost
                                          connection, a permission, a missing table, or the
                                          database refusing the data (each has a matching
                                          db_insert_failed ERROR log)
    db_rows_dropped_total                 rows given up on: lost after the final failed
                                          attempt, or refused by the database one by one

The Ticketmaster API key travels in the request URL (`?apikey=`), so it must never
reach a log line: failures are logged from sanitized fields only (see `_fetch_events`).
"""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import signal
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

from lv_db_connector import Connector, DbEnvCredentials
from lv_kafka_connector import KafkaEnvCredentials, KafkaProducer
from lv_telemetry_connector import configure_telemetry, get_logger


# =============================================================================
# Configuration — edit these defaults or override via the environment.
# =============================================================================

load_dotenv()

SERVICE = os.getenv("SERVICE_NAME", "ticketmaster2kafka")

# Upstream
TM_BASE_URL = os.getenv("TM_BASE_URL", "https://app.ticketmaster.com")
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "30"))
TM_POLL_MINS = int(os.getenv("TM_POLL_MINS", "60"))

# Retries for ONE page of results: 429, 5xx, timeouts and connection errors are tried
# up to FETCH_MAX_ATTEMPTS times in total, sleeping FETCH_BACKOFF_BASE_S * 2^(n-1)
# seconds (with +-50% jitter, capped at FETCH_BACKOFF_MAX_S) after the n-th failure.
# Other 4xx (a bad key, a bad query) and unreadable 200s are never retried: asking
# again cannot change the answer. When the last attempt fails the cycle is abandoned
# and the hourly loop carries on.
FETCH_MAX_ATTEMPTS = 3
FETCH_BACKOFF_BASE_S = 2.0
FETCH_BACKOFF_MAX_S = 30.0

# Fixed query params for the Nashville geo window (geohash + radius). The radius is 1 MILE on
# purpose. The Discovery API names this parameter "unit" (miles | km, default miles). Earlier
# versions sent "units": "km", a name the docs do not list; the API appears to ignore unknown
# parameters (not testable without a key), so the window in practice was 1 mile and that is the
# size we keep. Saying "unit": "miles" outright makes it the documented behavior and stops it
# depending on an unknown parameter being tolerated. Use "unit": "km" for a ~1.6x smaller disc.
DEFAULT_QUERY_PARAMS: Dict[str, Any] = {"geoPoint": "dn6m9qgn", "radius": 1, "unit": "miles"}

# Outputs
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nashville.ticketmaster.events")
DB_TABLE = os.getenv("DB_TABLE", "geo_feeds.ticketmaster_events")

# Seconds to wait, at the end of each batch, for the broker to acknowledge it.
KAFKA_FLUSH_TIMEOUT_S = float(os.getenv("KAFKA_FLUSH_TIMEOUT_S", "10"))
# A failing topic fails every message; log it at most once per reason per this many seconds.
KAFKA_ERROR_LOG_INTERVAL_S = 60.0

nashville_tz = ZoneInfo('US/Central')


def now_dtz():
    return dt.datetime.now(tz=nashville_tz)


def _iso_utc(dt: datetime) -> str:
    # Ticketmaster requires UTC with 'Z'
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _redacted_traceback() -> str:
    """traceback.format_exc() with the Ticketmaster API key masked out.

    Belt and braces for the places that log an UNEXPECTED exception. The fetch path
    never lets a requests exception (whose text embeds the full URL, key included) get
    this far, but a future bug must not be able to write the key to Loki. Masks both
    the `apikey=<value>` query-string form and the literal key value.
    """
    text = traceback.format_exc()
    text = re.sub(r"(?i)(api[_-]?key=)[^&\s\"'<>]+", r"\1<redacted>", text)
    key = os.environ.get("TICKETMASTER_API_KEY")
    if key:
        text = text.replace(key, "<redacted>")
    return text


# =============================================================================
# Database connector — subclass `lv_db_connector.Connector`.
# =============================================================================
#
# All SQL for this service lives here; the producer below just calls
# db.insert_events(rows).

class TicketmasterDb(Connector):
    """Postgres connector for the ticketmaster_events hypertable."""

    def insert_events(self, rows: List[dict]) -> None:
        """Insert one row per event. Column set matches sql/ticketmaster_events.sql."""
        self.insert(DB_TABLE, rows)


def _db_refused_the_data(exc: Exception) -> bool:
    """True when an insert failed because the DATABASE refused the rows themselves.

    That is SQLSTATE class 22 (data exception: a malformed date, text that is not a
    number, ...), class 23 (integrity violation: NOT NULL, a duplicate key) or 42804
    (datatype mismatch: a list where a scalar column wants a number). Retrying the same
    rows can never help, but inserting them one at a time can tell the good rows from
    the bad. Everything else (class 08 connection, 28 and the rest of 42 such as a
    permission or a missing table or column, 53/57 resources, no SQLSTATE at all) is
    about the database or the connection, not about one row, so the rows are not
    retried one by one: that would just fail N more times.

    The connector re-raises psycopg errors as LvDbQueryError / LvDbBulkInsertError
    `from` the original, so the SQLSTATE sits on `__cause__` (and on the exception
    itself in the connector's own LvDbQueryCanceledError); both places are checked.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc.__cause__, "sqlstate", None) or ""
    return sqlstate.startswith(("22", "23")) or sqlstate == "42804"


# =============================================================================
# Feed — fetch, flatten, dispatch.
# =============================================================================

class TicketmasterEventsProducer:
    def __init__(self, base_url: str, poll_interval_minutes: int, *,
                 kafka: KafkaProducer, db: TicketmasterDb, tel,
                 query_params: Dict[str, Any] | None = None):
        self.base_url = base_url.rstrip('/')
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kafka = kafka
        self.db = db
        self._log = tel.get_logger(self.__class__.__name__)
        self.topic_name = KAFKA_TOPIC
        self.partition_key = "0"
        self.api_key = os.environ['TICKETMASTER_API_KEY']
        # urllib3 is the one library that logs request URLs, and its WARNING lines
        # (e.g. "Failed to parse headers (url=...)") carry the full query string,
        # key included; at DEBUG it logs every request. Nothing it says is worth that,
        # and our own upstream_fetch_failed line covers failures, so silence it. Done
        # here, next to where the key is loaded. (50 = logging.CRITICAL.)
        get_logger("urllib3").setLevel(50)

        # fetch params
        self.page_size = int(os.environ.get('TM_PAGE_SIZE', '200'))
        # A COPY: the caller passes the module-level DEFAULT_QUERY_PARAMS, and this
        # class must never write into it.
        self.base_params: Dict[str, Any] = dict(query_params or {})
        if 'countryCode' not in self.base_params:
            self.base_params['countryCode'] = os.environ.get('TM_COUNTRY_CODE', 'US')
        # optional absolute time bounds — when set they pin the window forever
        if os.environ.get('TM_START_ISO'):
            self.base_params['startDateTime'] = os.environ['TM_START_ISO']
        if os.environ.get('TM_END_ISO'):
            self.base_params['endDateTime'] = os.environ['TM_END_ISO']
        # If neither absolute bound was provided the window is a rolling one: it is
        # recomputed from the clock on EVERY fetch cycle (see _fetch_events), never
        # stored. (It used to be computed once here, so the service went blind after
        # TM_WINDOW_DAYS of uptime.)
        self.window_is_fixed = ("startDateTime" in self.base_params
                                or "endDateTime" in self.base_params)
        self.window_days = int(os.environ.get("TM_WINDOW_DAYS", "28"))

        # optional extra params: "classificationName=music,city=Nashville".
        # Applied last each cycle, so they can override anything above.
        self.extra_params: Dict[str, Any] = {}
        extra = os.environ.get('TM_EXTRA_PARAMS')
        if extra:
            for kv in extra.split(','):
                if '=' in kv:
                    k,v = kv.split('=',1)
                    self.extra_params[k.strip()] = v.strip()

        # Kafka delivery failures are logged at most once per reason per
        # KAFKA_ERROR_LOG_INTERVAL_S: reason -> [monotonic time of last log, lines
        # suppressed since]. The delivery callback runs on whichever thread calls
        # poll()/flush() (the worker normally, the main thread at shutdown), so the
        # state is guarded by a lock.
        self._kafka_log_state: Dict[str, list] = {}
        self._kafka_log_lock = threading.Lock()

        self._fetched_total = tel.counter(
            "events_fetched_total",
            "Events fetched from the Ticketmaster Discovery API.",
        )
        self._emitted_total = tel.counter(
            "events_emitted_total",
            "Events ENQUEUED to Kafka (accepted by the local producer queue, not "
            "necessarily delivered; see events_delivered_total).",
        )
        self._delivered_total = tel.counter(
            "events_delivered_total",
            "Messages the Kafka broker acknowledged",
        )
        self._delivery_errors_total = tel.counter(
            "kafka_delivery_errors_total",
            "Kafka messages that failed to enqueue or to be delivered, by reason "
            "(librdkafka error name, or enqueue_error).",
            labelnames=("reason",),
        )
        self._fetch_latency = tel.histogram(
            "fetch_seconds",
            "Wall-clock time of the upstream Ticketmaster paging run.",
        )
        self._fetch_errors_total = tel.counter(
            "fetch_errors_total",
            "Failed attempts to fetch a page from the Ticketmaster Discovery API, by reason.",
            labelnames=("reason",),
        )
        self._last_fetch_success = tel.gauge(
            "last_fetch_success_timestamp_seconds",
            "Unix time of the last fetch cycle that got every page.",
        )
        self._duplicates_skipped_total = tel.counter(
            "duplicates_skipped_total",
            "Events skipped for the database because the same id appeared more than "
            "once in a cycle (the last occurrence is kept).",
        )
        self._invalid_total = tel.counter(
            "events_invalid_total",
            "Events that never reached the database, by reason: missing_id, missing_name, "
            "malformed (skipped before the insert) or db_rejected (the database refused "
            "the row; every other row of the cycle was still inserted).",
            labelnames=("reason",),
        )
        self._db_write_errors_total = tel.counter(
            "db_write_errors_total",
            "Failed database inserts, whatever the cause (connection, permission, a missing "
            "table, or the database refusing the data): one per failed attempt of the whole "
            "cycle, plus one when the row-by-row pass hits a failure that is not about the row.",
        )
        self._db_rows_dropped_total = tel.counter(
            "db_rows_dropped_total",
            "Rows given up on: lost after the final failed insert attempt, or refused one by "
            "one by the database (the latter are also in events_invalid_total{reason=db_rejected}).",
        )
        # Create the labelled series up front so they read 0 (not "absent") before the
        # first failure; an absent series cannot be alerted on with increase().
        for reason in ("timeout", "connection", "http_429", "http_4xx", "http_5xx",
                       "invalid_response"):
            self._fetch_errors_total.labels(reason=reason)
        for reason in ("missing_id", "missing_name", "malformed", "db_rejected"):
            self._invalid_total.labels(reason=reason)
        self._delivery_errors_total.labels(reason="enqueue_error")

    def wait(self):
        """Sleep the poll interval in 1 s slices, serving Kafka delivery callbacks
        between slices (a batch's late acknowledgements and failures are only
        reported when someone polls, and this service is otherwise idle for an hour).

        poll() RAISES once librdkafka has hit a fatal producer error (for example a
        missing cluster-level permission for idempotent writes: the producer is then
        unusable until the process restarts). That must not kill the worker, which
        would also stop the independent database sink; it is counted and logged like
        any other Kafka failure, so a stuck producer is loud (a steadily rising
        kafka_delivery_errors_total{reason="poll_error"}), not silent."""
        deadline = time.monotonic() + self.poll_interval_seconds
        while not _shutdown:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                self.kafka.poll(0)
            except Exception as exc:
                self._note_kafka_failure("poll_error", f"{type(exc).__name__}: {exc}", None)
            _sleep_responsively(min(1.0, remaining))

    # ---- API ----
    def _fetch_events(self) -> List[Dict[str, Any]] | None:
        """Page through events.json for one cycle; returns every event, or None.

        None means "no usable result this cycle": a page could not be fetched
        after FETCH_MAX_ATTEMPTS attempts (or failed in a way retrying cannot fix),
        or SIGTERM arrived mid-fetch. Partial results are never returned, since a
        half-fetched snapshot in Postgres would look like events vanishing. By the time
        None comes back every failed attempt has already been counted and logged, so
        the caller just waits for the next cycle.

        The query window is computed here, from the clock, on every call (see
        __init__); TM_START_ISO / TM_END_ISO pin it when set.

        KEY SAFETY: the API key is sent as ?apikey=..., and the text of every
        requests exception (and of raise_for_status) embeds the full URL. So nothing
        below ever logs, formats or re-raises a requests exception: each failure is
        reduced to its class name, the status code and the URL's host and path
        (never its query string) before it is logged.
        """
        # ---- the window, fresh for this cycle ----
        params = dict(self.base_params)
        if not self.window_is_fixed:
            now = datetime.now(timezone.utc)
            params["startDateTime"] = _iso_utc(now)
            params["endDateTime"] = _iso_utc(now + timedelta(days=self.window_days))
        params.update(self.extra_params)
        window = {"start": params.get("startDateTime"), "end": params.get("endDateTime"),
                  "fixed": self.window_is_fixed}
        self._log.info("fetch_window", extra=window)

        url = f"{self.base_url}/discovery/v2/events.json"
        upstream = urlsplit(url).hostname      # host only: no userinfo, no port
        path = urlsplit(url).path              # path only: never the query string
        size = max(1, min(200, self.page_size))
        collected: List[Dict[str, Any]] = []
        pages = 0
        page = 0
        with self._fetch_latency.time():
            while size * page < 1000:          # guard deep paging
                if _shutdown:
                    return None

                # ---- one page, up to FETCH_MAX_ATTEMPTS tries ----
                for attempt in range(1, FETCH_MAX_ATTEMPTS + 1):
                    reason = None              # None means this attempt succeeded
                    error_class = None
                    status_code = None
                    events: Any = []
                    has_next = False
                    try:
                        r = requests.get(
                            url,
                            params=dict(params, apikey=self.api_key, size=size, page=page),
                            timeout=HTTP_TIMEOUT_S,
                        )
                        status_code = r.status_code
                    except requests.Timeout as exc:          # ConnectTimeout and ReadTimeout
                        reason, error_class = "timeout", type(exc).__name__
                    except requests.RequestException as exc:  # refused, DNS, reset, TLS, ...
                        reason, error_class = "connection", type(exc).__name__

                    if reason is None:
                        if status_code == 429:
                            reason, error_class = "http_429", "HTTPError"
                        elif status_code >= 500:
                            reason, error_class = "http_5xx", "HTTPError"
                        elif status_code >= 400:
                            reason, error_class = "http_4xx", "HTTPError"
                        else:
                            # r.json() raises requests' JSONDecodeError, itself a
                            # RequestException, so it must not share the try above.
                            try:
                                doc = r.json()
                                events = (doc.get("_embedded") or {}).get("events", [])
                                has_next = "next" in (doc.get("_links") or {})
                                if not isinstance(events, list):
                                    raise TypeError("events is not a list")
                            except (ValueError, AttributeError, TypeError) as exc:
                                reason, error_class = "invalid_response", type(exc).__name__

                    if reason is None:
                        break

                    will_retry = (reason not in ("http_4xx", "invalid_response")
                                  and attempt < FETCH_MAX_ATTEMPTS)
                    delay = None
                    if will_retry:
                        delay = min(FETCH_BACKOFF_MAX_S,
                                    FETCH_BACKOFF_BASE_S * 2 ** (attempt - 1)
                                    * random.uniform(0.5, 1.5))
                    self._fetch_errors_total.labels(reason=reason).inc()
                    self._log.error("upstream_fetch_failed", extra={
                        "upstream": upstream, "path": path, "status_code": status_code,
                        "error_class": error_class, "reason": reason, "attempt": attempt,
                        "max_attempts": FETCH_MAX_ATTEMPTS, "page": page,
                        "retry_in_s": round(delay, 1) if delay is not None else None,
                    })
                    if not will_retry:
                        self._log.warning("fetch_cycle_abandoned", extra={
                            "reason": reason, "page": page, "events_so_far": len(collected)})
                        return None
                    _sleep_responsively(delay)
                    if _shutdown:
                        return None

                collected.extend(events)
                pages += 1
                if not has_next:
                    break
                page += 1

        self._fetched_total.inc(len(collected))
        self._last_fetch_success.set(time.time())
        self._log.info("fetch_complete", extra={"events": len(collected), "pages": pages, **window})
        if not collected:
            # A window that returns nothing is not an error to Ticketmaster, so say it
            # out loud: this is what a window gone stale (or a wrong geo filter) looks like.
            self._log.warning("fetch_returned_no_events", extra=window)
        return collected

    # ---- Mapping (flatten to ticketmaster_events row) ----
    @staticmethod
    def _pick_primary_image(images: List[Dict[str,Any]]) -> str | None:
        if not images: return None
        best = None; best_w = -1
        for im in images:
            w = im.get("width") or 0
            if im.get("ratio") == "16_9" and w > best_w:
                best = im; best_w = w
        if not best:
            for im in images:
                w = im.get("width") or 0
                if w > best_w:
                    best = im; best_w = w
        return best.get("url") if best else None

    @staticmethod
    def _pick_prices(e: dict) -> tuple[str | None, float | None, float | None]:
        pr = e.get("priceRanges") or []
        if not pr: return (None, None, None)
        std = next((x for x in pr if x.get("type") == "standard"), pr[0])
        return (std.get("currency"), std.get("min"), std.get("max"))

    @staticmethod
    def _flatten_event(e: dict) -> dict:
        dates = e.get("dates") or {}
        start  = (dates.get("start") or {})
        sales  = ((e.get("sales") or {}).get("public") or {})
        emb    = (e.get("_embedded") or {})
        venues = emb.get("venues") or []
        v0     = venues[0] if venues else {}
        city   = (v0.get("city") or {}).get("name")
        state  = (v0.get("state") or {})
        country= (v0.get("country") or {})
        atts   = emb.get("attractions") or []
        att_names = [a.get("name") for a in atts if a.get("name")]
        attraction_primary = att_names[0] if att_names else None
        attraction_names = "; ".join(att_names) if att_names else None
        cls    = e.get("classifications") or []
        c0     = next((c for c in cls if c.get("primary")), (cls[0] if cls else {}))
        img_url = TicketmasterEventsProducer._pick_primary_image(e.get("images") or [])
        currency, pmin, pmax = TicketmasterEventsProducer._pick_prices(e)

        return {
            "id": e["id"],
            "name": e.get("name"),
            "url": e.get("url"),
            "source": e.get("source"),
            "locale": e.get("locale"),
            "test": bool(e.get("test", False)),
            "status_code": (dates.get("status") or {}).get("code"),
            "timezone": dates.get("timezone"),
            "start_local_date": start.get("localDate"),
            "start_local_time": start.get("localTime"),
            "start_datetime_utc": start.get("dateTime"),
            "onsale_start_utc": sales.get("startDateTime"),
            "onsale_end_utc": sales.get("endDateTime"),
            "venue_id": v0.get("id"),
            "venue_name": v0.get("name"),
            "venue_address_line1": (v0.get("address") or {}).get("line1"),
            "city_name": city,
            "state_code": state.get("stateCode"),
            "country_code": country.get("countryCode"),
            "venue_postal_code": v0.get("postalCode"),
            "venue_timezone": v0.get("timezone"),
            "venue_lat": (v0.get("location") or {}).get("latitude"),
            "venue_lon": (v0.get("location") or {}).get("longitude"),
            "attraction_primary": attraction_primary,
            "attraction_names": attraction_names,
            "class_segment": (c0.get("segment") or {}).get("name"),
            "class_genre": (c0.get("genre") or {}).get("name"),
            "class_subgenre": (c0.get("subGenre") or {}).get("name"),
            "class_type": (c0.get("type") or {}).get("name"),
            "class_subtype": (c0.get("subType") or {}).get("name"),
            "image_url_primary": img_url,
            "price_currency": currency,
            "price_min": pmin,
            "price_max": pmax,
        }

    # ---- DB insert ----
    def insert_events(self, events: List[dict]):
        """Write one snapshot row per distinct, valid event; never raises on bad data.

        Every row of a cycle shares one write_time and the primary key is
        (id, write_time), and the connector inserts the whole batch in ONE
        transaction (executemany below 500 rows, COPY at 500 and above). So one row
        the database refuses (a repeated id, a null name, a malformed date, a NUL
        character) would roll back every row of the cycle. Three lines of defence,
        cheapest first:

          1. Building the rows. Events with no usable id or name, or that cannot be
             flattened, are skipped (counted and logged). Text has NUL and unencodable
             characters removed (Postgres text cannot hold them); a date or time that
             does not parse becomes NULL (logged as event_fields_sanitized). Repeated
             ids are collapsed, keeping the LAST occurrence.
          2. The insert, retried once on failure (the pool hands out a fresh
             connection on the retry, which is what a stale idle connection needs).
          3. If the DATABASE refuses the data itself (see _db_refused_the_data) the
             rows go in one at a time, so only the rows it refuses are lost.

        Accounting (the one rule: every failure is counted AND logged at ERROR with its
        exception class and message, so an alert on the counters and a search of the
        logs always agree):

          * every failed attempt of the whole-cycle insert, whatever the cause (a lost
            connection, a permission, a missing table, or the database refusing the data),
            is one db_write_errors_total and one db_insert_failed ERROR;
          * rows given up on are db_rows_dropped_total: all of them after the final
            failed attempt, or each row the database refused in the row-by-row pass
            (those are also events_invalid_total{reason="db_rejected"}, which tells a
            data problem from a connection problem, and are listed, with their error
            class and message, in one db_rows_rejected ERROR);
          * a row-by-row pass that hits a failure that is NOT about the row (the
            connection dropped, a permission went away) stops, because every row after
            it would fail the same way: that is one more db_write_errors_total and
            db_insert_failed, and the row and all after it are db_rows_dropped_total.

        Nothing is raised: the hourly loop just carries on. Kafka has already received
        the events by now, so nothing here may hold that back.
        """
        now = now_dtz()  # tz-aware now

        def _f(v):
            try:
                return float(v) if v not in (None, "") else None
            except Exception:
                return None

        rows_by_id: Dict[str, dict] = {}   # insertion-ordered; a repeat overwrites in place
        duplicates = 0
        invalid: Dict[str, int] = {}
        invalid_sample: List[dict] = []
        sanitized: List[dict] = []         # one entry per value changed while building rows
        for ev in events:
            ev_id = ev.get("id") if isinstance(ev, dict) else None
            ev_name = ev.get("name") if isinstance(ev, dict) else None
            reason = None
            flat: dict = {}
            if not isinstance(ev_id, str) or not ev_id:
                reason = "missing_id"
            elif not isinstance(ev_name, str):
                reason = "missing_name"
            else:
                try:
                    flat = self._flatten_event(ev)
                    flat["venue_lat"] = _f(flat.get("venue_lat"))
                    flat["venue_lon"] = _f(flat.get("venue_lon"))
                    # Dates and times are parsed here: Postgres would refuse a value like
                    # "TBD" and with it the whole batch. What does not parse becomes NULL.
                    for col in ("start_local_date", "start_local_time", "start_datetime_utc",
                                "onsale_start_utc", "onsale_end_utc"):
                        raw = flat[col]
                        if raw in (None, ""):
                            flat[col] = None
                            continue
                        try:
                            if col == "start_local_date":
                                flat[col] = dt.date.fromisoformat(raw)
                            elif col == "start_local_time":
                                flat[col] = dt.time.fromisoformat(raw)
                            else:
                                flat[col] = dt.datetime.fromisoformat(raw)
                        except (TypeError, ValueError):
                            flat[col] = None
                            sanitized.append({"id": ev_id, "field": col, "fix": "set_null",
                                              "value": str(raw)[:40]})
                    # Postgres text cannot hold NUL, and a lone surrogate (which JSON can
                    # carry) cannot be encoded as UTF-8: either one fails the whole insert.
                    for col, value in flat.items():
                        if isinstance(value, str):
                            clean = value.replace("\x00", "").encode("utf-8", "replace").decode("utf-8")
                            if clean != value:
                                flat[col] = clean
                                sanitized.append({"id": ev_id, "field": col, "fix": "text_cleaned"})
                except Exception:
                    reason = "malformed"
            if reason is not None:
                invalid[reason] = invalid.get(reason, 0) + 1
                self._invalid_total.labels(reason=reason).inc()
                if len(invalid_sample) < 5:
                    invalid_sample.append({
                        "reason": reason,
                        "id": ev_id if isinstance(ev_id, str) else None,
                        "name": ev_name[:80] if isinstance(ev_name, str) else None,
                    })
                continue
            if flat["id"] in rows_by_id:
                duplicates += 1
            rows_by_id[flat["id"]] = {
                "write_time": now,
                "first_seen_utc": now,
                "last_seen_utc": now,
                **flat
            }

        if invalid:
            self._log.warning("invalid_events_skipped", extra={
                "count": sum(invalid.values()), "by_reason": invalid, "sample": invalid_sample})
        if sanitized:
            self._log.warning("event_fields_sanitized", extra={
                "count": len(sanitized), "sample": sanitized[:5]})
        if duplicates:
            self._duplicates_skipped_total.inc(duplicates)
            self._log.info("duplicate_events_skipped", extra={"count": duplicates})

        rows = list(rows_by_id.values())
        if not rows:
            self._log.info("db_insert_skipped",
                           extra={"reason": "no_valid_rows", "events": len(events)})
            return

        # ---- 1) the whole cycle in one insert, retried once ----
        inserted = 0
        refused = None          # the exception, when the database refused the data itself
        for attempt in (1, 2):
            try:
                self.db.insert_events(rows)
                inserted = len(rows)
                break
            except Exception as exc:
                # Every failed attempt is counted and logged, whatever the cause. When
                # the database refused the data itself a retry cannot help, so the next
                # step is the row-by-row pass instead (and the rows are only "dropped"
                # there, one by one, if the database really refuses them).
                self._db_write_errors_total.inc()
                refused_the_data = _db_refused_the_data(exc)
                will_retry = attempt == 1 and not refused_the_data
                if not will_retry and not refused_the_data:
                    self._db_rows_dropped_total.inc(len(rows))
                self._log.error("db_insert_failed", extra={
                    "table": DB_TABLE, "rows": len(rows), "attempt": attempt,
                    "will_retry": will_retry, "will_isolate_rows": refused_the_data,
                    "row_by_row": False,
                    "error_class": type(exc).__name__, "error": str(exc)[:500]})
                if refused_the_data:
                    refused = exc
                    break

        # ---- 2) the database refused some row: insert one at a time to find which ----
        rejected: List[dict] = []
        if refused is not None:
            for i, row in enumerate(rows):
                try:
                    self.db.insert_events([row])
                except Exception as exc:
                    if _db_refused_the_data(exc):
                        self._invalid_total.labels(reason="db_rejected").inc()
                        self._db_rows_dropped_total.inc()
                        rejected.append({"id": row["id"], "error_class": type(exc).__name__,
                                         "error": str(exc)[:200]})
                        continue
                    # Not about this row (the connection dropped, a permission went
                    # away): every row after it would fail the same way, so stop here
                    # and count this row and the rest as dropped.
                    self._db_write_errors_total.inc()
                    self._db_rows_dropped_total.inc(len(rows) - i)
                    self._log.error("db_insert_failed", extra={
                        "table": DB_TABLE, "rows": len(rows) - i, "attempt": 1,
                        "will_retry": False, "will_isolate_rows": False, "row_by_row": True,
                        "error_class": type(exc).__name__, "error": str(exc)[:500]})
                    break
                inserted += 1
        if rejected:
            # ERROR, not WARNING: these rows are lost to the database for good (they are
            # in db_rows_dropped_total). With every row refused (schema drift: a new NOT
            # NULL column, a changed type) `inserted` is 0 and the whole sink is dead.
            self._log.error("db_rows_rejected", extra={
                "table": DB_TABLE, "count": len(rejected), "rows": len(rows),
                "inserted": inserted, "sample": rejected[:5],
                "error_class": type(refused).__name__, "batch_error": str(refused)[:500]})

        # One INSERT per row per cycle (never an upsert): the table is an append-only
        # history of snapshots, one row per event per poll.
        if inserted:
            self._log.info("db_rows_inserted", extra={
                "table": DB_TABLE, "rows": inserted, "events": len(events),
                "duplicates_skipped": duplicates, "invalid_skipped": sum(invalid.values()),
                "rejected": len(rejected)})

    # ---- Kafka ----
    def _note_kafka_failure(self, reason: str, error: str, partition: int | None) -> None:
        """Count one failed Kafka message and log it, at most once a minute per reason.

        Called from the delivery callback (any thread) and from the enqueue handler.
        A dead topic fails every message of a batch, so the ERROR line is rate-limited
        and carries `suppressed`, the number of lines dropped since the last one. The
        counter is NOT rate-limited. Never raises.
        """
        try:
            self._delivery_errors_total.labels(reason=reason).inc()
            now = time.monotonic()
            with self._kafka_log_lock:
                state = self._kafka_log_state.setdefault(reason, [None, 0])
                if state[0] is not None and now - state[0] < KAFKA_ERROR_LOG_INTERVAL_S:
                    state[1] += 1
                    return
                suppressed = state[1]
                state[0], state[1] = now, 0
            self._log.error("kafka_delivery_failed", extra={
                "topic": self.topic_name, "reason": reason, "error": error,
                "partition": partition, "suppressed": suppressed})
        except Exception:
            pass

    def _on_delivery(self, err, msg) -> None:
        """Delivery callback passed to every produce().

        Runs on a librdkafka-served thread (whichever calls poll() or flush()), so it
        is short, thread-safe and never raises: a failing callback would be swallowed
        by librdkafka and the failure would go uncounted.
        """
        try:
            if err is None:
                self._delivered_total.inc()
                return
            try:
                partition = msg.partition()
            except Exception:
                partition = None
            self._note_kafka_failure(err.name(), str(err), partition)
        except Exception:
            pass

    def produce_events_to_kafka(self, events):
        enqueued = 0
        enqueue_failed = 0
        for e in events:
            try:
                payload = {"source":"ticketmaster","fetched_at": time.time(),"event": e}
                # The wire format is a JSON *string* holding the JSON payload (the
                # old kafka_confluent wrapper json-encoded what it was handed).
                # External consumers depend on it, so the json.dumps() here is
                # deliberate — do not remove it.
                #
                # raise_on_error=True: without it the connector SWALLOWS enqueue
                # errors (queue full, produce rejected) and only counts them in its
                # own metric. The delivery callback reports what happens after the
                # enqueue.
                self.kafka.produce(
                    self.topic_name,
                    value=json.dumps(payload),
                    key=self.partition_key,
                    headers={'service': b'ticketmaster', 'datatype': b'event'},
                    on_delivery=self._on_delivery,
                    raise_on_error=True,
                )
            except Exception as exc:
                enqueue_failed += 1
                self._note_kafka_failure("enqueue_error", f"{type(exc).__name__}: {exc}", None)
                continue
            enqueued += 1
            self._emitted_total.inc()

        # Wait for the broker to acknowledge the batch; flush() also serves the
        # delivery callbacks. A missing topic or an ACL denial is only ever visible
        # through those callbacks, never through produce() or flush()'s return value
        # (flush returns 0 once every message has FAILED too), so the counters above
        # are the signal and this warning only covers "still in flight".
        try:
            remaining = self.kafka.flush(KAFKA_FLUSH_TIMEOUT_S)
        except Exception as exc:        # a fatal producer error raises here too (see wait())
            self._note_kafka_failure("flush_error", f"{type(exc).__name__}: {exc}", None)
            remaining = 0
        if remaining:
            self._log.warning("kafka_flush_incomplete", extra={"queued": remaining})
        self._log.info("kafka_batch_enqueued", extra={
            "topic": self.topic_name, "enqueued": enqueued, "enqueue_failed": enqueue_failed})


# --- Orchestration function ---
def update_ticketmaster_events(base_url, poll_interval_minutes, kafka, db, tel,
                               query_params: dict | None = None):
    log = tel.get_logger("update_ticketmaster_events")
    tm = TicketmasterEventsProducer(base_url, poll_interval_minutes, kafka=kafka, db=db,
                                    tel=tel, query_params=query_params)
    log.info("producer_created")
    while not _shutdown:
        # 1) pull. Retries, counters and logs for upstream failures live inside
        #    _fetch_events; None means "nothing usable this cycle".
        try:
            events = tm._fetch_events()
        except Exception as exc:
            log.error("fetch_cycle_failed", extra={
                "error_class": type(exc).__name__, "traceback": _redacted_traceback()})
            events = None
        if events is None:
            tm.wait()
            continue

        # Debug-only peek at the first event. Fenced off so that a malformed event
        # can never cost the cycle (this used to run inside the fetch's try).
        try:
            if events and log.isEnabledFor(10):   # 10 = DEBUG
                log.debug("first_event", extra={"event": json.dumps(events[0]),
                                                "flattened": tm._flatten_event(events[0])})
        except Exception:
            pass

        # 2) produce to Kafka
        try:
            tm.produce_events_to_kafka(events)
        except Exception as exc:
            log.error("kafka_batch_failed", extra={
                "error_class": type(exc).__name__, "traceback": _redacted_traceback()})

        # 3) insert to DB
        try:
            tm.insert_events(events)
        except Exception as exc:
            log.error("db_batch_failed", extra={
                "error_class": type(exc).__name__, "traceback": _redacted_traceback()})

        tm.wait()


# =============================================================================
# Lifecycle — graceful shutdown.
# =============================================================================

_shutdown = False
_worker_failed = False


def _on_signal(_signum, _frame) -> None:
    """SIGTERM / SIGINT handler. Flip the flag; the poll loop notices."""
    global _shutdown
    _shutdown = True


def _connector_versions() -> dict[str, str]:
    """Version of every lv_* connector this process has imported, for the `connectors`
    startup log line.

    The connectors are not version-pinned: an image gets whatever each connector repo's
    default branch held when it was built, so this line is the only way to tell, from a
    running service, which connector code it actually has. It reads each module's own
    `__version__` (what is running), not pip's metadata, which an editable install
    freezes at install time. A version is not a commit: two builds can share one.
    """
    return {
        name: str(getattr(module, "__version__", "unknown"))
        for name, module in sorted(sys.modules.items())
        if name.startswith("lv_") and "." not in name
    }


def _sleep_responsively(seconds: float) -> None:
    """Sleep in small chunks so SIGTERM is responsive.

    Never `time.sleep(poll_interval)` directly — k8s will SIGTERM and wait
    `terminationGracePeriodSeconds` (default 30 s) before SIGKILL. A
    multi-minute sleep (this service polls hourly) means we miss the SIGTERM
    and the pod is killed hard.
    """
    deadline = time.monotonic() + seconds
    while not _shutdown:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(0.5, remaining))


# Helper function to wrap thread targets for fatal error handling
def thread_wrapper(target_func, args=(), name="", log=None):
    def wrapped():
        global _shutdown, _worker_failed
        try:
            target_func(*args)
        except Exception as exc:
            # The traceback goes through _redacted_traceback(): this is the last
            # line of defence against a secret reaching the logs.
            log.critical("worker_thread_crashed", extra={
                "worker": name, "error_class": type(exc).__name__,
                "traceback": _redacted_traceback()})
            _worker_failed = True
            _shutdown = True
    return wrapped


# =============================================================================
# Main.
# =============================================================================


def main() -> None:
    tel = configure_telemetry(service=SERVICE)
    log = tel.get_logger("main")

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("connectors", extra={"connectors": _connector_versions()})

    log.info(
        "startup",
        extra={
            "service": SERVICE,
            "upstream": TM_BASE_URL,
            "poll_interval_mins": TM_POLL_MINS,
            "topic": KAFKA_TOPIC,
            "db_table": DB_TABLE,
        },
    )
    # KAFKA_TOPIC_BASENAME held a full topic name (it was misnamed); it is now
    # KAFKA_TOPIC. A deployment still setting the old one would silently publish to the
    # default topic instead of the topic it thinks, so say so.
    if os.getenv("KAFKA_TOPIC_BASENAME"):
        log.warning("legacy_env_ignored", extra={
            "env_var": "KAFKA_TOPIC_BASENAME", "replaced_by": "KAFKA_TOPIC",
            "topic_in_use": KAFKA_TOPIC})

    with (
        KafkaProducer(KafkaEnvCredentials()) as kafka,
        TicketmasterDb(DbEnvCredentials(), persistent=True) as db,
    ):
        log.info("worker_starting")
        worker = threading.Thread(
            target=thread_wrapper(
                update_ticketmaster_events,
                args=(
                    TM_BASE_URL,            # base_url
                    TM_POLL_MINS,           # poll interval (minutes)
                    kafka,
                    db,
                    tel,
                    DEFAULT_QUERY_PARAMS,
                ),
                name="ticketmaster_events",
                log=log,
            ),
            name="ticketmaster_events",
            # Daemon, so a worker stuck in a slow HTTP call cannot keep the process
            # alive past the join below when SIGTERM arrives.
            daemon=True,
        )
        worker.start()

        # Signals are only delivered to the main thread, so it waits here and
        # lets the worker notice `_shutdown` on its next check.
        while not _shutdown and worker.is_alive():
            time.sleep(0.5)
        worker.join(timeout=30)

    log.info("shutdown")
    if _worker_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
