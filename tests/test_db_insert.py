"""Per-event isolation and failure accounting for the database insert (TM-06, TM-15, TM-17 log).

All rows of a cycle share one write_time, the primary key is (id, write_time), and the
connector inserts the batch in one transaction with no ON CONFLICT: ONE repeated id, one
event without a name (name is NOT NULL), one event without an id, a NUL character or a
date that is not a date used to roll back the entire cycle. These tests drive
TicketmasterEventsProducer.insert_events with a fake DB and pin what it hands the
connector, including the row-by-row fallback when the database refuses the data.
(The real-database behaviour, executemany and COPY included, needs TimescaleDB and is not
part of this no-infrastructure suite; it was proven by hand against a throwaway container.)
"""

from __future__ import annotations

import datetime as dt
import logging
import re
from pathlib import Path

import pytest

import ticketmaster2kafka as t
from conftest import FakeRequests, FakeResponse, make_event, page_doc, pg_error

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def tm(make_producer):
    return make_producer(dict(t.DEFAULT_QUERY_PARAMS))


def inserted(db):
    assert len(db.calls) == 1, "expected exactly one insert call"
    return db.calls[0]


def test_the_invalid_event_series_exist_at_zero_before_any_failure(tm, tel):
    """An absent series cannot be alerted on with increase(); db_rejected is one of them."""
    assert {dict(k)["reason"] for k in tel.metrics["events_invalid_total"].values} == {
        "missing_id", "missing_name", "malformed", "db_rejected"}
    assert all(v == 0 for v in tel.metrics["events_invalid_total"].values.values())


# ----- dedupe --------------------------------------------------------------------


def test_a_repeated_id_inserts_once_and_keeps_the_last_occurrence(tm, db, tel):
    events = [make_event("dup", name="first"), make_event("other", name="x"),
              make_event("dup", name="second"), make_event("dup", name="third")]
    tm.insert_events(events)
    rows = inserted(db)
    assert sorted(r["id"] for r in rows) == ["dup", "other"]
    assert {r["id"]: r["name"] for r in rows}["dup"] == "third"
    assert tel.value("duplicates_skipped_total") == 2


def test_no_duplicates_means_no_duplicate_counter_or_log(tm, db, tel, caplog):
    caplog.set_level(logging.DEBUG)
    tm.insert_events([make_event("a"), make_event("b")])
    assert tel.value("duplicates_skipped_total") == 0
    assert not [r for r in caplog.records if r.getMessage() == "duplicate_events_skipped"]


# ----- invalid events ------------------------------------------------------------


def test_events_without_id_or_name_are_skipped_counted_and_logged(tm, db, tel, caplog):
    caplog.set_level(logging.DEBUG)
    no_id = make_event("tmp", name="Nameless Id")
    del no_id["id"]
    no_name = make_event("noname")
    no_name["name"] = None
    missing_name_key = make_event("nokey")
    del missing_name_key["name"]
    events = [make_event("good1"), no_id, no_name, missing_name_key, make_event("good2"),
              {"id": ""}, "not even a dict", None]

    tm.insert_events(events)

    assert sorted(r["id"] for r in inserted(db)) == ["good1", "good2"]
    assert tel.value("events_invalid_total", reason="missing_id") == 4      # no id, "", str, None
    assert tel.value("events_invalid_total", reason="missing_name") == 2
    (warn,) = [r for r in caplog.records if r.getMessage() == "invalid_events_skipped"]
    assert warn.levelno == logging.WARNING
    assert warn.count == 6 and warn.by_reason == {"missing_id": 4, "missing_name": 2}
    assert 1 <= len(warn.sample) <= 5
    assert {s["id"] for s in warn.sample} >= {"noname"}


def test_an_event_that_cannot_be_flattened_is_skipped_not_fatal(tm, db, tel):
    bad = make_event("weird", dates=["not", "a", "dict"])
    tm.insert_events([make_event("good"), bad])
    assert [r["id"] for r in inserted(db)] == ["good"]
    assert tel.value("events_invalid_total", reason="malformed") == 1


def test_a_valid_event_is_kept_when_a_later_duplicate_of_it_is_invalid(tm, db):
    nameless_dup = make_event("same")
    nameless_dup["name"] = None
    tm.insert_events([make_event("same", name="keeper"), nameless_dup])
    (row,) = inserted(db)
    assert row["name"] == "keeper"


def test_nothing_valid_means_no_insert_call(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    tm.insert_events([{"name": "no id"}])
    tm.insert_events([])
    assert db.calls == []
    skipped = [r for r in caplog.records if r.getMessage() == "db_insert_skipped"]
    assert len(skipped) == 2


# ----- rows ----------------------------------------------------------------------


def test_row_shape_matches_the_ddl_exactly(tm, db):
    tm.insert_events([make_event("a")])
    (row,) = inserted(db)
    ddl = (ROOT / "sql" / "ticketmaster_events.sql").read_text()
    body = ddl.split("CREATE TABLE IF NOT EXISTS geo_feeds.ticketmaster_events (")[1].split("PRIMARY KEY")[0]
    columns = re.findall(r"^\s{4}([a-z_0-9]+)\s+[A-Z]", body, flags=re.M)
    assert set(row) == set(columns), (set(row) ^ set(columns))
    assert len(columns) == 37
    assert row["venue_lat"] == pytest.approx(36.159) and row["venue_lon"] == pytest.approx(-86.778)
    assert row["write_time"] == row["first_seen_utc"] == row["last_seen_utc"]
    # the date and time columns are handed over parsed, not as the upstream's strings
    assert row["start_local_date"] == dt.date(2026, 10, 31)
    assert row["start_local_time"] == dt.time(20, 0)
    assert row["start_datetime_utc"] == dt.datetime(2026, 11, 1, 1, 0, tzinfo=dt.timezone.utc)
    assert row["onsale_start_utc"] == dt.datetime(2026, 8, 1, 15, 0, tzinfo=dt.timezone.utc)
    assert row["onsale_end_utc"] == row["start_datetime_utc"]


def test_all_rows_of_a_cycle_share_one_write_time(tm, db):
    tm.insert_events([make_event("a"), make_event("b"), make_event("c")])
    assert len({r["write_time"] for r in inserted(db)}) == 1


def test_bad_coordinates_become_null_not_an_error(tm, db):
    ev = make_event("a")
    ev["_embedded"]["venues"][0]["location"] = {"latitude": "n/a", "longitude": ""}
    tm.insert_events([ev])
    (row,) = inserted(db)
    assert row["venue_lat"] is None and row["venue_lon"] is None


# ----- values the database would refuse: sanitized while the row is built -------------


def sanitize_lines(caplog):
    return [r for r in caplog.records if r.getMessage() == "event_fields_sanitized"]


def test_nul_characters_are_stripped_from_every_text_column(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    ev = make_event("a", name="Bridge\x00stone Show")
    ev["_embedded"]["venues"][0]["name"] = "Arena\x00"
    ev["_embedded"]["attractions"] = [{"name": "\x00The Band"}]
    tm.insert_events([ev, make_event("b")])
    rows = {r["id"]: r for r in inserted(db)}
    assert rows["a"]["name"] == "Bridgestone Show"
    assert rows["a"]["venue_name"] == "Arena"
    assert rows["a"]["attraction_primary"] == rows["a"]["attraction_names"] == "The Band"
    assert not any("\x00" in v for r in rows.values() for v in r.values() if isinstance(v, str))
    (warn,) = sanitize_lines(caplog)
    assert warn.levelno == logging.WARNING and warn.count == 4
    assert {(s["id"], s["field"], s["fix"]) for s in warn.sample} == {
        ("a", "name", "text_cleaned"), ("a", "venue_name", "text_cleaned"),
        ("a", "attraction_primary", "text_cleaned"), ("a", "attraction_names", "text_cleaned")}


def test_a_lone_surrogate_is_replaced_because_utf8_cannot_encode_it(tm, db):
    """JSON can carry "\\ud800"; psycopg then fails to encode the string (a raw
    UnicodeEncodeError, no SQLSTATE) and the whole insert goes with it."""
    tm.insert_events([make_event("a", name="Half \ud800 pair"), make_event("b")])
    rows = {r["id"]: r for r in inserted(db)}
    assert rows["a"]["name"] == "Half ? pair"
    rows["a"]["name"].encode("utf-8")                 # encodable now


def test_an_id_that_changes_when_cleaned_is_deduplicated_by_its_clean_form(tm, db):
    tm.insert_events([make_event("same"), make_event("same\x00", name="second")])
    (row,) = inserted(db)                              # one row, not two with the same key
    assert row["id"] == "same" and row["name"] == "second"


@pytest.mark.parametrize("field, path", [
    ("start_local_date", ("dates", "start", "localDate")),
    ("start_local_time", ("dates", "start", "localTime")),
    ("start_datetime_utc", ("dates", "start", "dateTime")),
    ("onsale_start_utc", ("sales", "public", "startDateTime")),
    ("onsale_end_utc", ("sales", "public", "endDateTime")),
])
@pytest.mark.parametrize("bad", ["TBD", "2026-13-45", "25:99:00", "not a time", 20261031, {"a": 1}])
def test_a_value_that_does_not_parse_becomes_null_and_the_event_is_kept(tm, db, caplog, field, path, bad):
    caplog.set_level(logging.DEBUG)
    ev = make_event("a")
    node = ev
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = bad
    tm.insert_events([ev, make_event("b")])
    rows = {r["id"]: r for r in inserted(db)}
    assert rows["a"][field] is None
    assert rows["b"][field] is not None                # the neighbour is untouched
    assert rows["a"]["name"] == "Fixture Event"        # and so is the rest of the row
    (warn,) = sanitize_lines(caplog)
    assert warn.count == 1
    assert warn.sample == [{"id": "a", "field": field, "fix": "set_null", "value": str(bad)[:40]}]


def test_missing_or_empty_dates_are_null_without_a_warning(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    ev = make_event("a")
    ev["dates"]["start"].pop("localTime")              # a date-only event
    ev["dates"]["start"]["localDate"] = ""             # an empty string is "absent" too
    tm.insert_events([ev])
    (row,) = inserted(db)
    assert row["start_local_time"] is None and row["start_local_date"] is None
    assert sanitize_lines(caplog) == []


def test_clean_events_produce_no_sanitize_warning(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    tm.insert_events([make_event("a"), make_event("b")])
    assert sanitize_lines(caplog) == []


def test_the_sanitize_sample_is_capped_but_the_count_is_exact(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    events = [make_event(f"e{i}") for i in range(20)]
    for ev in events:
        ev["dates"]["start"]["localDate"] = "TBD"
    tm.insert_events(events)
    (warn,) = sanitize_lines(caplog)
    assert warn.count == 20 and len(warn.sample) == 5
    assert len(inserted(db)) == 20


# ----- the database refuses the data anyway: row by row ---------------------------------


@pytest.mark.parametrize("sqlstate, message", [
    ("22007", "invalid input syntax for type date"),     # data exception
    ("22P02", "invalid input syntax for type numeric"),
    ("22003", "numeric value out of range"),
    ("23502", "null value violates not-null constraint"),   # integrity violation
    ("23505", "duplicate key value violates unique constraint"),
    ("23514", "violates check constraint"),
    ("42804", "column price_min is of type numeric but expression is of type numeric[]"),
])
@pytest.mark.parametrize("n", [12, 600])       # below and above the connector's COPY threshold
def test_a_refused_row_costs_one_row_not_the_cycle(tm, db, tel, caplog, sqlstate, message, n):
    caplog.set_level(logging.DEBUG)
    bad = {"e3", "e7", "e11"}
    db.refuse = lambda rows: pg_error(sqlstate, message) if bad & {r["id"] for r in rows} else None
    tm.insert_events([make_event(f"e{i}") for i in range(n)])

    assert sorted(r["id"] for r in db.stored) == sorted(f"e{i}" for i in range(n) if f"e{i}" not in bad)
    assert len(db.calls[0]) == n and all(len(c) == 1 for c in db.calls[1:])
    assert len(db.calls) == 1 + n                  # the whole cycle ONCE (no whole-cycle retry), then one by one

    # Counted as what it is: ONE failed batch, and the three rows the database turned down
    # are rows given up on. events_invalid_total{db_rejected} says it was the data.
    assert tel.value("events_invalid_total", reason="db_rejected") == 3
    assert tel.value("db_write_errors_total") == 1
    assert tel.value("db_rows_dropped_total") == 3

    # ... and logged at ERROR with the exception class and message, once for the batch and
    # once for the rows (never once per row: a dead sink would be 600 lines a poll).
    (failed,) = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert failed.levelno == logging.ERROR and failed.table == t.DB_TABLE
    assert failed.rows == n and failed.attempt == 1
    assert failed.will_retry is False and failed.will_isolate_rows is True and failed.row_by_row is False
    assert failed.error_class == "LvDbQueryError" and message in failed.error
    (rejected,) = [r for r in caplog.records if r.getMessage() == "db_rows_rejected"]
    assert rejected.levelno == logging.ERROR and rejected.table == t.DB_TABLE
    assert rejected.count == 3 and rejected.rows == n and rejected.inserted == n - 3
    assert rejected.error_class == "LvDbQueryError" and message in rejected.batch_error
    assert [s["id"] for s in rejected.sample] == ["e3", "e7", "e11"]
    assert all(s["error_class"] == "LvDbQueryError" and message in s["error"] for s in rejected.sample)
    assert sorted(r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR) == [
        "db_insert_failed", "db_rows_rejected"]
    (ok,) = [r for r in caplog.records if r.getMessage() == "db_rows_inserted"]
    assert ok.rows == n - 3 and ok.rejected == 3 and ok.events == n


def test_the_rejected_id_sample_is_capped_and_the_counter_is_exact(tm, db, tel, caplog):
    caplog.set_level(logging.DEBUG)
    db.refuse = lambda rows: pg_error("22P02") if any(int(r["id"][1:]) % 2 for r in rows) else None
    tm.insert_events([make_event(f"e{i}") for i in range(20)])
    assert tel.value("events_invalid_total", reason="db_rejected") == 10
    assert tel.value("db_rows_dropped_total") == 10
    (warn,) = [r for r in caplog.records if r.getMessage() == "db_rows_rejected"]
    assert warn.count == 10 and len(warn.sample) == 5
    assert len(db.stored) == 10


def test_a_list_in_a_scalar_column_costs_that_row_not_the_cycle(tm, db, tel):
    """price_min is NUMERIC; a list there reaches Postgres as numeric[] and is refused with
    42804 (datatype mismatch, class 42), which is not a class 22 or 23 error. It must still
    be isolated to its own row instead of costing the cycle's rows."""
    odd = make_event("odd")
    odd["priceRanges"] = [{"type": "standard", "currency": "USD", "min": [1, 2], "max": 5}]
    db.refuse = lambda rows: (pg_error("42804", "column price_min is of type numeric but expression "
                                                "is of type numeric[]")
                              if any(isinstance(r["price_min"], list) for r in rows) else None)
    tm.insert_events([make_event("a"), make_event("b"), odd, make_event("c"), make_event("d")])
    assert sorted(r["id"] for r in db.stored) == ["a", "b", "c", "d"]
    assert tel.value("events_invalid_total", reason="db_rejected") == 1
    assert tel.value("db_rows_dropped_total") == 1


def test_every_row_refused_is_a_dead_sink_and_says_so_in_counters_and_errors(tm, db, tel, caplog):
    """Schema drift (a DBA adds a NOT NULL column without a default, changes a type, drops a
    default): the database refuses EVERY row, every poll. That used to move only
    events_invalid_total{db_rejected} and log a WARNING, so an alert on db_write_errors_total
    or db_rows_dropped_total, or a search for ERROR lines, never fired: 84 events fetched and
    delivered to Kafka, 0 rows in the table, and the counters those alerts watch at 0."""
    caplog.set_level(logging.DEBUG)
    message = 'null value in column "must_have" of relation "tm_drift" violates not-null constraint'
    db.refuse = lambda rows: pg_error("23502", message)
    tm.insert_events([make_event(f"e{i}") for i in range(84)])

    assert db.stored == []
    assert tel.value("db_write_errors_total") == 1            # the failed batch
    assert tel.value("db_rows_dropped_total") == 84           # every row given up on
    assert tel.value("events_invalid_total", reason="db_rejected") == 84
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert sorted(r.getMessage() for r in errors) == ["db_insert_failed", "db_rows_rejected"]
    assert all(r.error_class == "LvDbQueryError" for r in errors)
    assert all("must_have" in (getattr(r, "error", "") + getattr(r, "batch_error", "")) for r in errors)
    (rejected,) = [r for r in errors if r.getMessage() == "db_rows_rejected"]
    assert rejected.count == 84 and rejected.inserted == 0 and len(rejected.sample) == 5
    assert not [r for r in caplog.records if r.getMessage() == "db_rows_inserted"]


def test_a_batch_the_database_refuses_but_that_isolates_clean_is_still_counted(tm, db, tel, caplog):
    """The whole-cycle insert was refused, yet every row goes in on its own (nothing for the
    database to refuse once alone). The failed batch is still a failure: one counter, one
    ERROR; but no row was lost, so nothing is dropped and there is no db_rows_rejected."""
    caplog.set_level(logging.DEBUG)
    db.failures = [pg_error("23505", "duplicate key value violates unique constraint")]
    tm.insert_events([make_event("a"), make_event("b")])
    assert sorted(r["id"] for r in db.stored) == ["a", "b"]
    assert tel.value("db_write_errors_total") == 1
    assert tel.value("db_rows_dropped_total") == 0
    assert tel.value("events_invalid_total", reason="db_rejected") == 0
    (failed,) = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert failed.getMessage() == "db_insert_failed" and failed.will_isolate_rows is True
    assert not [r for r in caplog.records if r.getMessage() == "db_rows_rejected"]


@pytest.mark.parametrize("scenario", [
    "refused_one_row", "refused_every_row", "connection_lost", "transient_then_ok",
    "transient_then_refused", "connection_drops_mid_row_by_row",
])
def test_every_counted_failed_insert_has_exactly_one_matching_error_log(tm, db, tel, caplog, scenario):
    """The invariant that makes the counter and the logs agree: db_write_errors_total moves
    by one for every db_insert_failed ERROR line, never more, never fewer."""
    caplog.set_level(logging.DEBUG)
    if scenario == "refused_one_row":
        db.refuse = lambda rows: pg_error("22P02") if "e1" in {r["id"] for r in rows} else None
    elif scenario == "refused_every_row":
        db.refuse = lambda rows: pg_error("23502")
    elif scenario == "connection_lost":
        db.failures = [pg_error("08006"), pg_error("08006")]
    elif scenario == "transient_then_ok":
        db.failures = [pg_error("08006")]
    elif scenario == "transient_then_refused":
        db.failures = [pg_error("08006")]
        db.refuse = lambda rows: pg_error("22P02") if "e1" in {r["id"] for r in rows} else None
    elif scenario == "connection_drops_mid_row_by_row":
        db.refuse = lambda rows: (pg_error("22P02") if len(rows) > 1 else
                                  pg_error("08006") if rows[0]["id"] == "e1" else None)
    tm.insert_events([make_event(f"e{i}") for i in range(4)])
    failures = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert len(failures) == tel.value("db_write_errors_total") >= 1
    assert all(r.levelno == logging.ERROR and r.error_class and r.error for r in failures)
    # and every row is accounted for: stored, or dropped (never silently lost)
    assert len(db.stored) + tel.value("db_rows_dropped_total") == 4


@pytest.mark.parametrize("sqlstate", [
    "08006",    # connection failure
    "08003",    # connection does not exist
    "28000",    # invalid authorization
    "42501",    # permission denied
    "42P01",    # undefined table
    "42703",    # undefined column
    "53200",    # out of memory
    "57014",    # query canceled (statement_timeout)
    "40001",    # serialization failure
    None,       # no SQLSTATE at all (a client-side failure)
])
def test_other_failures_are_retried_whole_once_and_never_row_by_row(tm, db, tel, caplog, sqlstate):
    """A connection or permission failure would fail every row the same way: isolating the
    rows would turn one failure into N more."""
    caplog.set_level(logging.DEBUG)
    db.failures = [pg_error(sqlstate, "no good"), pg_error(sqlstate, "no good")]
    rows = [make_event(f"e{i}") for i in range(30)]
    tm.insert_events(rows)
    assert len(db.calls) == 2 and all(len(c) == 30 for c in db.calls)
    assert tel.value("db_write_errors_total") == 2
    assert tel.value("db_rows_dropped_total") == 30
    assert tel.value("events_invalid_total", reason="db_rejected") == 0
    errors = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert [(e.attempt, e.will_retry, e.row_by_row) for e in errors] == [(1, True, False), (2, False, False)]


def test_a_transient_failure_then_a_refusal_is_still_isolated(tm, db, tel):
    """The first attempt hit a stale connection (retried); the retry reached the database,
    which then refused one row."""
    db.failures = [pg_error("08006")]
    db.refuse = lambda rows: pg_error("22007") if "e1" in {r["id"] for r in rows} else None
    tm.insert_events([make_event("e0"), make_event("e1"), make_event("e2")])
    assert sorted(r["id"] for r in db.stored) == ["e0", "e2"]
    assert tel.value("db_write_errors_total") == 2             # the stale connection, then the refused batch
    assert tel.value("db_rows_dropped_total") == 1             # e1
    assert tel.value("events_invalid_total", reason="db_rejected") == 1


def test_row_by_row_stops_at_the_first_failure_that_is_not_about_the_row(tm, db, tel, caplog):
    """Refused rows are skipped, but when the connection itself fails mid-way the rest are
    not attempted (each would fail the same way): this row and everything after it are
    counted as dropped, once."""
    caplog.set_level(logging.DEBUG)

    def refuse(rows):
        ids = {r["id"] for r in rows}
        if len(rows) > 1 and "e2" in ids:                 # the whole cycle: refused because of e2
            return pg_error("22P02")
        if ids == {"e2"}:
            return pg_error("22P02")
        if ids == {"e6"}:                                 # then the connection drops
            return pg_error("08006", "server closed the connection unexpectedly")
        return None

    db.refuse = refuse
    tm.insert_events([make_event(f"e{i}") for i in range(10)])

    assert sorted(r["id"] for r in db.stored) == ["e0", "e1", "e3", "e4", "e5"]
    assert [len(c) for c in db.calls] == [10] + [1] * 7        # e0..e6, and e7..e9 never tried
    assert tel.value("events_invalid_total", reason="db_rejected") == 1
    assert tel.value("db_write_errors_total") == 2              # the refused batch, then the dropped connection
    assert tel.value("db_rows_dropped_total") == 5              # e2 (refused), then e6, e7, e8, e9
    failures = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert len(failures) == 2 and all(r.levelno == logging.ERROR for r in failures)
    batch, err = failures
    assert batch.row_by_row is False and batch.will_isolate_rows is True and batch.rows == 10
    assert err.row_by_row is True and err.rows == 4 and err.will_isolate_rows is False
    assert err.attempt == 1 and err.will_retry is False and "server closed" in err.error
    (rej,) = [r for r in caplog.records if r.getMessage() == "db_rows_rejected"]
    assert rej.count == 1 and rej.inserted == 5
    (ok,) = [r for r in caplog.records if r.getMessage() == "db_rows_inserted"]
    assert ok.rows == 5 and ok.rejected == 1


def test_one_poll_adds_at_most_three_to_db_write_errors_total(tm, db, tel, caplog):
    """The README tells whoever writes the alert that a poll adds at most 3: the first
    attempt (a stale connection), the retry (the database refuses a row), and the
    row-by-row pass (the connection drops again). Every one has its own ERROR line."""
    caplog.set_level(logging.DEBUG)
    db.failures = [pg_error("08006", "stale connection")]

    def refuse(rows):
        if len(rows) > 1:
            return pg_error("22P02", "invalid input syntax")          # the whole cycle: a bad row somewhere
        if rows[0]["id"] == "e2":
            return pg_error("08006", "server closed the connection")  # the pass loses the connection
        return None

    db.refuse = refuse
    tm.insert_events([make_event(f"e{i}") for i in range(4)])
    assert sorted(r["id"] for r in db.stored) == ["e0", "e1"]
    assert tel.value("db_write_errors_total") == 3
    assert tel.value("db_rows_dropped_total") == 2                     # e2 and e3, never tried
    assert tel.value("events_invalid_total", reason="db_rejected") == 0
    failures = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert [(f.attempt, f.will_retry, f.will_isolate_rows, f.row_by_row) for f in failures] == [
        (1, True, False, False), (2, False, True, False), (1, False, False, True)]


@pytest.mark.parametrize("make_exc, expected", [
    (lambda: pg_error("22007"), True),                 # data exception
    (lambda: pg_error("22P02"), True),
    (lambda: pg_error("23502"), True),                 # integrity violation
    (lambda: pg_error("23505"), True),
    (lambda: pg_error("23514"), True),
    (lambda: pg_error("42804"), True),                 # datatype mismatch: a list in a scalar column
    (lambda: pg_error("08006"), False),                # connection
    (lambda: pg_error("42501"), False),                # permission
    (lambda: pg_error("42P01"), False),                # undefined table
    (lambda: pg_error("42703"), False),                # undefined column
    (lambda: pg_error("42883"), False),                # undefined function
    (lambda: pg_error("53300"), False),                # too many connections
    (lambda: pg_error("57014"), False),                # statement timeout
    (lambda: pg_error(None), False),                   # a psycopg error with no SQLSTATE
    (lambda: RuntimeError("boom"), False),             # not a database error at all
    (lambda: UnicodeEncodeError("utf-8", "x", 0, 1, "surrogates not allowed"), False),
])
def test_only_these_sqlstates_count_as_the_database_refusing_the_data(make_exc, expected):
    assert t._db_refused_the_data(make_exc()) is expected


def test_the_sqlstate_is_also_read_from_the_exception_itself():
    """LvDbQueryCanceledError carries .sqlstate directly; a future connector may do that
    for every query error."""
    direct = RuntimeError("x")
    direct.sqlstate = "22003"
    assert t._db_refused_the_data(direct) is True
    direct.sqlstate = "57014"
    assert t._db_refused_the_data(direct) is False


# ----- the insert itself ---------------------------------------------------------


def test_the_success_log_is_accurate_not_inserted_or_updated(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    tm.insert_events([make_event("a"), make_event("a"), make_event("b")])
    messages = [r.getMessage() for r in caplog.records]
    assert "db_rows_inserted" in messages
    assert not any("Inserted/updated" in m for m in messages)
    (rec,) = [r for r in caplog.records if r.getMessage() == "db_rows_inserted"]
    assert rec.rows == 2 and rec.events == 3 and rec.duplicates_skipped == 1 and rec.table == t.DB_TABLE
    assert rec.rejected == 0


def test_a_failed_insert_is_retried_once_and_then_succeeds(tm, db, tel, caplog):
    caplog.set_level(logging.DEBUG)
    db.failures = [RuntimeError("server closed the connection unexpectedly")]
    tm.insert_events([make_event("a"), make_event("b")])
    assert len(db.calls) == 2 and db.calls[0] == db.calls[1]
    assert tel.value("db_write_errors_total") == 1
    assert tel.value("db_rows_dropped_total") == 0
    (err,) = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert err.levelno == logging.ERROR and err.will_retry is True and err.attempt == 1
    assert err.row_by_row is False
    assert err.error_class == "RuntimeError" and "closed the connection" in err.error
    assert [r for r in caplog.records if r.getMessage() == "db_rows_inserted"]


def test_two_failed_attempts_drop_the_rows_counted_and_do_not_raise(tm, db, tel, caplog):
    caplog.set_level(logging.DEBUG)
    db.failures = [RuntimeError("boom 1"), RuntimeError("boom 2")]
    tm.insert_events([make_event("a"), make_event("b"), make_event("c")])      # must not raise
    assert len(db.calls) == 2
    assert tel.value("db_write_errors_total") == 2
    assert tel.value("db_rows_dropped_total") == 3
    errors = [r for r in caplog.records if r.getMessage() == "db_insert_failed"]
    assert [(e.attempt, e.will_retry) for e in errors] == [(1, True), (2, False)]
    assert errors[1].error == "boom 2" and errors[1].rows == 3
    assert not [r for r in caplog.records if r.getMessage() == "db_rows_inserted"]


def test_a_long_error_message_is_truncated(tm, db, caplog):
    caplog.set_level(logging.DEBUG)
    db.failures = [RuntimeError("x" * 5000)] * 2
    tm.insert_events([make_event("a")])
    assert all(len(r.error) == 500 for r in caplog.records if r.getMessage() == "db_insert_failed")


# ----- the cycle as a whole ------------------------------------------------------


def test_a_malformed_first_event_cannot_cost_the_cycle(monkeypatch, tel, kafka, db, caplog):
    """The debug log used to flatten events[0] eagerly inside the fetch's try: a first
    event without an id raised KeyError there and skipped Kafka AND the database."""
    caplog.set_level(logging.DEBUG)                     # debug logging ON, the failing case
    first = make_event("x")
    del first["id"]
    monkeypatch.setattr(t.requests, "get", FakeRequests(
        [FakeResponse(200, page_doc([first, make_event("good1"), make_event("good2")]))]))
    monkeypatch.setattr(t.TicketmasterEventsProducer, "wait",
                        lambda self: monkeypatch.setattr(t, "_shutdown", True))

    t.update_ticketmaster_events("https://app.ticketmaster.com", 60, kafka, db, tel,
                                 dict(t.DEFAULT_QUERY_PARAMS))

    assert len(kafka.produced) == 3                                    # Kafka got everything
    assert sorted(r["id"] for r in inserted(db)) == ["good1", "good2"]  # the DB got the valid ones
    assert not [r for r in caplog.records if r.getMessage() in ("fetch_cycle_failed", "first_event")
                and r.levelno >= logging.ERROR]


def test_a_failing_debug_log_cannot_cost_the_cycle(monkeypatch, tel, kafka, db, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event("a")]))]))
    monkeypatch.setattr(t.TicketmasterEventsProducer, "_flatten_event",
                        staticmethod(lambda e: (_ for _ in ()).throw(RuntimeError("flatten broke"))))
    monkeypatch.setattr(t.TicketmasterEventsProducer, "wait",
                        lambda self: monkeypatch.setattr(t, "_shutdown", True))
    t.update_ticketmaster_events("https://app.ticketmaster.com", 60, kafka, db, tel,
                                 dict(t.DEFAULT_QUERY_PARAMS))
    assert len(kafka.produced) == 1                    # Kafka still got it
    assert db.calls == []                              # (the row itself is "malformed" now)
    assert tel.value("events_invalid_total", reason="malformed") == 1
