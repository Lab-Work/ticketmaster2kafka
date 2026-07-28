"""ticketmaster2kafka — Ticketmaster Discovery events to Kafka and Postgres.

Pages the Ticketmaster Discovery API for events in the configured geo window,
produces one message per event to Kafka, and writes a flattened row per event
to the `laddms.tm_events` TimescaleDB hypertable (see
`ticketmaster_tables.sql`).

    Run:        python ticketmaster2kafka.py
    Logs:       JSON on stdout (Loki-friendly)
    Metrics:    /metrics endpoint on :9100 (Prometheus)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import numpy as np
import requests
from dotenv import load_dotenv

from lv_db_connector import Connector, DbEnvCredentials
from lv_kafka_connector import KafkaEnvCredentials, KafkaProducer
from lv_telemetry_connector import configure_telemetry


# =============================================================================
# Configuration — edit these defaults or override via the environment.
# =============================================================================

load_dotenv()

SERVICE = os.getenv("SERVICE_NAME", "ticketmaster2kafka")

# Upstream
TM_BASE_URL = os.getenv("TM_BASE_URL", "https://app.ticketmaster.com")
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "30"))
TM_POLL_MINS = int(os.getenv("TM_POLL_MINS", "60"))

# Fixed query params for the Nashville geo window (geohash + radius).
DEFAULT_QUERY_PARAMS: Dict[str, Any] = {"geoPoint": "dn6m9qgn", "radius": 1, "units": "km"}

# Outputs
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_BASENAME", "nashville-tm")
DB_TABLE = os.getenv("DB_TABLE", "laddms.tm_events")

nashville_tz = ZoneInfo('US/Central')


def now_dtz():
    return dt.datetime.now(tz=nashville_tz)


def _iso_utc(dt: datetime) -> str:
    # Ticketmaster requires UTC with 'Z'
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# Database connector — subclass `lv_db_connector.Connector`.
# =============================================================================
#
# All SQL for this service lives here; the producer below just calls
# db.insert_events(rows).

class TicketmasterDb(Connector):
    """Postgres connector for the tm_events hypertable."""

    def insert_events(self, rows: List[dict]) -> None:
        """Insert one row per event. Column set matches ticketmaster_tables.sql."""
        self.insert(DB_TABLE, rows)


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
        # fetch params
        self.page_size = int(os.environ.get('TM_PAGE_SIZE', '200'))
        self.query_params: Dict[str, Any] = query_params or {}
        if 'countryCode' not in self.query_params:
            self.query_params['countryCode'] = os.environ.get('TM_COUNTRY_CODE', 'US')
        # optional time window — absolute bounds win
        if os.environ.get('TM_START_ISO'):
            self.query_params['startDateTime'] = os.environ['TM_START_ISO']
        if os.environ.get('TM_END_ISO'):
            self.query_params['endDateTime'] = os.environ['TM_END_ISO']

        # If neither absolute bound provided, derive from window
        if "startDateTime" not in self.query_params and "endDateTime" not in self.query_params:
            days_ahead = int(os.environ.get("TM_WINDOW_DAYS", "28"))
            now = datetime.now(timezone.utc)
            end = now + timedelta(days=days_ahead)
            # If you want to align to midnight UTC, uncomment next two lines:
            # now = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            # end = datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc)
            self.query_params["startDateTime"] = _iso_utc(now)
            self.query_params["endDateTime"]   = _iso_utc(end)

        # optional extra params: "classificationName=music,city=Nashville"
        extra = os.environ.get('TM_EXTRA_PARAMS')
        if extra:
            for kv in extra.split(','):
                if '=' in kv:
                    k,v = kv.split('=',1)
                    self.query_params[k.strip()] = v.strip()

        self._fetched_total = tel.counter(
            "events_fetched_total",
            "Events fetched from the Ticketmaster Discovery API.",
        )
        self._emitted_total = tel.counter(
            "events_emitted_total",
            "Events produced to kafka.",
        )
        self._fetch_latency = tel.histogram(
            "fetch_seconds",
            "Wall-clock time of the upstream Ticketmaster paging run.",
        )

    # style parity: small wait helper — responsive to SIGTERM
    def wait(self):
        _sleep_responsively(self.poll_interval_seconds)

    # ---- API ----
    def _fetch_events(self) -> List[Dict[str, Any]]:
        """Page through events.json (guard deep paging)."""
        collected: List[Dict[str, Any]] = []
        size = max(1, min(200, self.page_size))
        page = 0
        with self._fetch_latency.time():
            while True:
                if size * page >= 1000:
                    break
                params = dict(self.query_params, apikey=self.api_key, size=size, page=page)
                url = f"{self.base_url}/discovery/v2/events.json"
                r = requests.get(url, params=params, timeout=HTTP_TIMEOUT_S)
                if r.status_code == 429:
                    self._log.warning("Ticketmaster rate limit hit (429). Backing off briefly.")
                    time.sleep(2)
                    continue
                r.raise_for_status()
                doc = r.json()
                events = (doc.get("_embedded") or {}).get("events", [])
                collected.extend(events)
                links = doc.get("_links", {})
                if "next" not in links:
                    break
                page += 1
        self._fetched_total.inc(len(collected))
        return collected

    # ---- Mapping (flatten to tm_events row) ----
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
        now = now_dtz()  # tz-aware now

        def _f(v):
            try:
                return float(v) if v not in (None, "") else None
            except Exception:
                return None

        rows = []
        for ev in events:
            flat = self._flatten_event(ev)
            flat["venue_lat"] = _f(flat.get("venue_lat"))
            flat["venue_lon"] = _f(flat.get("venue_lon"))

            rows.append({
                "write_time": now,
                "first_seen_utc": now,
                "last_seen_utc": now,
                **flat
            })

        if not rows:
            self._log.info("No Ticketmaster events to insert this cycle.")
            return

        self.db.insert_events(rows)
        self._log.info(f"Inserted/updated {len(rows)} rows into {DB_TABLE}.")

    # ---- Kafka ----
    def produce_events_to_kafka(self, events):
        count = 0
        for e in events:
            payload = {"source":"ticketmaster","fetched_at": time.time(),"event": e}
            # The wire format is a JSON *string* holding the JSON payload (the
            # old kafka_confluent wrapper json-encoded what it was handed).
            # External consumers depend on it, so the json.dumps() here is
            # deliberate — do not remove it.
            self.kafka.produce(
                self.topic_name,
                value=json.dumps(payload),
                key=self.partition_key,
                headers={'service': b'ticketmaster', 'datatype': b'event'},
            )
            count += 1
            self._emitted_total.inc()
        self._log.info(f"Produced {count} Ticketmaster events to Kafka.")


# --- Orchestration function ---
def update_ticketmaster_events(base_url, poll_interval_minutes, kafka, db, tel,
                               query_params: dict | None = None):
    log = tel.get_logger("update_ticketmaster_events")
    tm = TicketmasterEventsProducer(base_url, poll_interval_minutes, kafka=kafka, db=db,
                                    tel=tel, query_params=query_params)
    log.info("Created new instance of Ticketmaster events receiver.")
    while not _shutdown:
        # 1) pull
        try:
            events = tm._fetch_events()
            log.info(f"Received {len(events)} events")
            if events:
                log.debug("first_event", extra={"event": json.dumps(events[0]),
                                                "flattened": tm._flatten_event(events[0])})
        except Exception as e:
            log.error("Failed to pull Ticketmaster events.")
            log.exception(e, exc_info=True)
            tm.wait()
            continue

        # 2) produce to Kafka
        try:
            tm.produce_events_to_kafka(events)
        except Exception as e:
            log.error("Failed to send Ticketmaster events to Kafka.")
            log.exception(e, exc_info=True)

        # 3) insert to DB
        try:
            tm.insert_events(events)
        except Exception as e:
            log.error("Failed to insert Ticketmaster events.")
            log.exception(e, exc_info=True)

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
        except Exception:
            log.critical(f"Unhandled exception in thread '{name}', exiting entire process.",
                         exc_info=True)
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

    with (
        KafkaProducer(KafkaEnvCredentials()) as kafka,
        TicketmasterDb(DbEnvCredentials(), persistent=True) as db,
    ):
        log.info("Starting Ticketmaster to Kafka producer thread.")
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
