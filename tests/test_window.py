"""The query window (TM-02): recomputed every cycle, pinned only by TM_START_ISO / TM_END_ISO.

The bug: startDateTime/endDateTime were computed once, in the producer's constructor, so
after TM_WINDOW_DAYS (28) of uptime the horizon reached zero and the service silently
fetched nothing. These tests run the REAL poll loop (update_ticketmaster_events) on a
fake clock for 45 simulated days, and fail if the window is ever computed once.
"""

from __future__ import annotations

import datetime as dt
import logging

import ticketmaster2kafka as t
from conftest import FakeRequests, FakeResponse, page_doc, make_event


def run_loop(monkeypatch, clock, tel, kafka, db, *, cycles, fake_get):
    """Run update_ticketmaster_events for `cycles` poll cycles, one simulated hour apart."""
    monkeypatch.setattr(t.requests, "get", fake_get)
    done = {"n": 0}

    def fake_wait(self):
        clock.advance(3600)
        done["n"] += 1
        if done["n"] >= cycles:
            monkeypatch.setattr(t, "_shutdown", True)

    monkeypatch.setattr(t.TicketmasterEventsProducer, "wait", fake_wait)
    t.update_ticketmaster_events("https://app.ticketmaster.com", 60, kafka, db, tel,
                                 dict(t.DEFAULT_QUERY_PARAMS))


def test_window_advances_every_cycle_over_45_simulated_days(monkeypatch, clock, tel, kafka, db):
    seen = []

    def fake_get(url, params=None, timeout=None):
        seen.append((params["startDateTime"], params["endDateTime"], clock.now))
        return FakeResponse(200, page_doc([make_event()]))

    cycles = 45 * 24
    run_loop(monkeypatch, clock, tel, kafka, db, cycles=cycles, fake_get=fake_get)

    assert len(seen) == cycles
    # one distinct (start, end) pair per cycle: it advances every single poll
    assert len({(s, e) for s, e, _ in seen}) == cycles
    for start, end, now in seen:
        # the window is exactly "now .. now + 28 days", in UTC with a Z
        assert start == now.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert end == (now + dt.timedelta(days=28)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # on the last simulated day the horizon is still 28 days (it used to be 0 and then negative)
    last_start, last_end, _ = seen[-1]
    horizon = dt.datetime.fromisoformat(last_end.replace("Z", "+00:00")) - dt.datetime.fromisoformat(
        last_start.replace("Z", "+00:00"))
    assert horizon == dt.timedelta(days=28)


def test_window_days_env_var_sets_the_horizon(monkeypatch, clock, make_producer):
    monkeypatch.setenv("TM_WINDOW_DAYS", "7")
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    make_producer(dict(t.DEFAULT_QUERY_PARAMS))._fetch_events()
    params = t.requests.get.calls[0]["params"]
    assert params["startDateTime"] == "2026-10-04T12:00:00Z"
    assert params["endDateTime"] == "2026-10-11T12:00:00Z"


def test_fixed_bounds_pin_the_window_for_the_whole_run(monkeypatch, clock, tel, kafka, db):
    monkeypatch.setenv("TM_START_ISO", "2026-12-01T00:00:00Z")
    monkeypatch.setenv("TM_END_ISO", "2026-12-31T00:00:00Z")
    seen = []

    def fake_get(url, params=None, timeout=None):
        seen.append((params["startDateTime"], params["endDateTime"]))
        return FakeResponse(200, page_doc([make_event()]))

    run_loop(monkeypatch, clock, tel, kafka, db, cycles=45 * 24, fake_get=fake_get)
    assert set(seen) == {("2026-12-01T00:00:00Z", "2026-12-31T00:00:00Z")}


def test_a_single_fixed_bound_wins_and_nothing_else_is_derived(monkeypatch, clock, make_producer):
    monkeypatch.setenv("TM_START_ISO", "2026-12-01T00:00:00Z")
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    make_producer(dict(t.DEFAULT_QUERY_PARAMS))._fetch_events()
    params = t.requests.get.calls[0]["params"]
    assert params["startDateTime"] == "2026-12-01T00:00:00Z"
    assert "endDateTime" not in params          # open-ended, exactly as before


def test_a_single_fixed_end_bound_wins_too_and_the_start_is_not_derived(monkeypatch, clock, make_producer):
    """TM_END_ISO alone pins the window exactly as TM_START_ISO alone does: it is sent
    unchanged on every cycle, and no startDateTime is computed from the clock."""
    monkeypatch.setenv("TM_END_ISO", "2026-12-31T00:00:00Z")
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    tm = make_producer(dict(t.DEFAULT_QUERY_PARAMS))
    assert tm.window_is_fixed is True
    tm._fetch_events()
    clock.advance(30 * 24 * 3600)               # a month later the window must not have moved
    tm._fetch_events()
    assert len(t.requests.get.calls) == 2
    for call in t.requests.get.calls:
        assert call["params"]["endDateTime"] == "2026-12-31T00:00:00Z"
        assert "startDateTime" not in call["params"]


def test_the_module_level_default_params_are_never_mutated(monkeypatch, clock, make_producer):
    before = dict(t.DEFAULT_QUERY_PARAMS)
    monkeypatch.setenv("TM_START_ISO", "2026-12-01T00:00:00Z")
    monkeypatch.setenv("TM_EXTRA_PARAMS", "classificationName=music")
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    tm = make_producer(t.DEFAULT_QUERY_PARAMS)
    tm._fetch_events()
    assert t.DEFAULT_QUERY_PARAMS == before
    # ...so a second producer built from the same dict starts from the same place
    monkeypatch.delenv("TM_START_ISO")
    tm2 = make_producer(t.DEFAULT_QUERY_PARAMS)
    assert tm2.window_is_fixed is False


def test_extra_params_are_sent_and_can_override(monkeypatch, clock, make_producer):
    monkeypatch.setenv("TM_EXTRA_PARAMS", "classificationName=music, city=Nashville")
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    make_producer(dict(t.DEFAULT_QUERY_PARAMS))._fetch_events()
    params = t.requests.get.calls[0]["params"]
    assert params["classificationName"] == "music" and params["city"] == "Nashville"
    assert params["geoPoint"] == "dn6m9qgn" and params["unit"] == "miles" and "units" not in params and params["radius"] == 1


def test_every_cycle_logs_the_window_it_used(monkeypatch, clock, make_producer, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    tm = make_producer(dict(t.DEFAULT_QUERY_PARAMS))
    tm._fetch_events()
    clock.advance(3600)
    tm._fetch_events()
    windows = [r for r in caplog.records if r.getMessage() == "fetch_window"]
    assert [(w.start, w.end, w.fixed) for w in windows] == [
        ("2026-10-04T12:00:00Z", "2026-11-01T12:00:00Z", False),
        ("2026-10-04T13:00:00Z", "2026-11-01T13:00:00Z", False),
    ]


def test_an_empty_cycle_warns_loudly(monkeypatch, clock, make_producer, tel, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([]))]))
    events = make_producer(dict(t.DEFAULT_QUERY_PARAMS))._fetch_events()
    assert events == []
    warn = [r for r in caplog.records if r.getMessage() == "fetch_returned_no_events"]
    assert len(warn) == 1 and warn[0].levelno == logging.WARNING
    assert warn[0].start == "2026-10-04T12:00:00Z" and warn[0].end == "2026-11-01T12:00:00Z"
    # an empty result is still a SUCCESSFUL fetch
    assert tel.value("last_fetch_success_timestamp_seconds") > 0


def test_a_non_empty_cycle_does_not_warn(monkeypatch, clock, make_producer, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(t.requests, "get", FakeRequests([FakeResponse(200, page_doc([make_event()]))]))
    make_producer(dict(t.DEFAULT_QUERY_PARAMS))._fetch_events()
    assert not [r for r in caplog.records if r.getMessage() == "fetch_returned_no_events"]
