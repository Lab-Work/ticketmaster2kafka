"""Names and startup: the renamed topic and table, the legacy-env warning, the DDL files.

The renames (Kafka topic nashville-tm -> nashville.ticketmaster.events, table
laddms.tm_events -> geo_feeds.ticketmaster_events) follow the other active feeds:
topics are <domain>.<source>.<event>, tables are geo_feeds.<feed>_<noun>.
"""

from __future__ import annotations

import ast
import importlib
import logging
import re
import signal
from pathlib import Path

import pytest

import ticketmaster2kafka as t
from conftest import FakeTel

ROOT = Path(__file__).resolve().parents[1]


def reload_with_env(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return importlib.reload(t)


@pytest.fixture(autouse=True)
def restore_module(monkeypatch):
    """Reloading re-reads the env, so put the env back BEFORE reloading the module for the
    next test (monkeypatch would otherwise undo it only after this teardown)."""
    yield
    monkeypatch.undo()
    importlib.reload(t)


def test_defaults_are_the_new_names(monkeypatch):
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    monkeypatch.delenv("DB_TABLE", raising=False)
    m = importlib.reload(t)
    assert m.KAFKA_TOPIC == "nashville.ticketmaster.events"
    assert m.DB_TABLE == "geo_feeds.ticketmaster_events"


def test_kafka_topic_env_var_overrides(monkeypatch):
    assert reload_with_env(monkeypatch, KAFKA_TOPIC="some.other.topic").KAFKA_TOPIC == "some.other.topic"


def test_the_legacy_topic_env_var_is_ignored_not_honoured(monkeypatch):
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    m = reload_with_env(monkeypatch, KAFKA_TOPIC_BASENAME="nashville-tm")
    assert m.KAFKA_TOPIC == "nashville.ticketmaster.events"


def run_main_with_fakes(monkeypatch):
    """main() with the connectors, telemetry and worker replaced; returns the FakeTel."""
    tel = FakeTel()

    class Ctx:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(t, "configure_telemetry", lambda service: tel)
    monkeypatch.setattr(t, "KafkaProducer", Ctx)
    monkeypatch.setattr(t, "TicketmasterDb", Ctx)
    monkeypatch.setattr(t, "KafkaEnvCredentials", lambda: None)
    monkeypatch.setattr(t, "DbEnvCredentials", lambda: None)
    monkeypatch.setattr(t, "update_ticketmaster_events", lambda *a, **k: None)
    old = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        t.main()                  # installs its own SIGTERM/SIGINT handlers
    finally:
        for s, handler in old.items():
            signal.signal(s, handler)


def test_main_warns_when_the_legacy_topic_env_var_is_still_set(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("KAFKA_TOPIC_BASENAME", "nashville-tm")
    run_main_with_fakes(monkeypatch)
    (warn,) = [r for r in caplog.records if r.getMessage() == "legacy_env_ignored"]
    assert warn.levelno == logging.WARNING
    assert warn.env_var == "KAFKA_TOPIC_BASENAME" and warn.replaced_by == "KAFKA_TOPIC"
    assert warn.topic_in_use == "nashville.ticketmaster.events"


def test_main_is_quiet_when_the_legacy_env_var_is_gone(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    run_main_with_fakes(monkeypatch)
    assert not [r for r in caplog.records if r.getMessage() == "legacy_env_ignored"]
    startup = [r for r in caplog.records if r.getMessage() == "startup"]
    assert startup[0].topic == "nashville.ticketmaster.events"
    assert startup[0].db_table == "geo_feeds.ticketmaster_events"


def test_the_worker_thread_is_a_daemon(monkeypatch):
    """A worker stuck in a slow HTTP call must not keep the process alive after SIGTERM."""
    seen = {}

    class FakeThread:
        def __init__(self, *a, daemon=False, **k): seen["daemon"] = daemon
        def start(self): pass
        def is_alive(self): return False
        def join(self, timeout=None): pass

    monkeypatch.setattr(t.threading, "Thread", FakeThread)
    run_main_with_fakes(monkeypatch)
    assert seen["daemon"] is True


# ----- the SQL files -----------------------------------------------------------------

OLD_NAMES = ("laddms", "tm_events", "v_tm_events_latest", "nashville-tm")


def test_the_ddl_uses_only_the_new_names():
    ddl = (ROOT / "sql" / "ticketmaster_events.sql").read_text()
    code = "\n".join(l for l in ddl.splitlines() if not l.lstrip().startswith("--"))
    for old in OLD_NAMES:
        assert old not in code, old
    assert "geo_feeds.ticketmaster_events" in ddl and "geo_feeds.v_ticketmaster_events_latest" in ddl
    assert "CREATE SCHEMA IF NOT EXISTS geo_feeds" in ddl
    assert re.search(r"create_hypertable\(\s*'geo_feeds\.ticketmaster_events',\s*'write_time',\s*if_not_exists => TRUE", ddl)
    for idx in ("start", "city", "geo", "last_seen"):
        assert f"ticketmaster_events_{idx}_idx" in ddl
    # a GRANT block for the app role exists but is commented out
    grants = [l for l in ddl.splitlines() if "GRANT" in l and "<app_role>" in l]
    assert grants and all(l.lstrip().startswith("--") for l in grants)


def test_the_old_ddl_file_is_gone():
    assert not (ROOT / "ticketmaster_tables.sql").exists()


def test_the_rename_script_guards_every_step():
    sql = (ROOT / "sql" / "rename_legacy.sql").read_text()
    assert sql.count("DO $$") == sql.count("$$;") == 5
    for step in ("step 1", "step 2", "step 3", "step 4", "step 5"):
        assert step in sql
    # every DDL statement sits inside a DO block that checks to_regclass() first
    assert sql.count("to_regclass(") >= 12
    assert "DROP TABLE" not in sql and "DELETE" not in sql and "TRUNCATE" not in sql


def test_each_rename_step_checks_the_old_object_exists_and_the_new_one_does_not():
    """A static pin (the script itself was run against throwaway Postgres containers by
    hand): in every step both guards come BEFORE the first statement that changes anything,
    so a re-run, or a half-migrated database, is refused or skipped rather than failed."""
    sql = (ROOT / "sql" / "rename_legacy.sql").read_text()
    blocks = re.split(r"(?m)^DO \$\$\n", sql)[1:]
    assert len(blocks) == 5
    guards = [
        ("to_regclass('laddms.tm_events') IS NULL", "to_regclass('geo_feeds.ticketmaster_events') IS NOT NULL"),
        ("to_regclass('geo_feeds.tm_events') IS NULL", "to_regclass('geo_feeds.ticketmaster_events') IS NOT NULL"),
        ("to_regclass('geo_feeds.tm_events_' || suffix) IS NULL",
         "to_regclass('geo_feeds.ticketmaster_events_' || suffix) IS NOT NULL"),
        ("to_regclass('laddms.v_tm_events_latest') IS NULL",
         "to_regclass('geo_feeds.v_ticketmaster_events_latest') IS NOT NULL"),
        ("to_regclass('geo_feeds.ticketmaster_events') IS NULL",
         "to_regclass('geo_feeds.v_ticketmaster_events_latest') IS NOT NULL"),
    ]
    for step, (block, (old_missing, new_exists)) in enumerate(zip(blocks, guards), start=1):
        first_change = re.search(r"\b(ALTER |CREATE (SCHEMA|VIEW)|EXECUTE )", block).start()
        for guard in (old_missing, new_exists):
            assert guard in block, f"step {step} lost its guard: {guard}"
            assert block.index(guard) < first_change, f"step {step}: {guard} must come before the change"


# The rename script is hand-run on production, so tests cannot run it (no Postgres here;
# it was proven against throwaway containers). What they CAN do is pin the facts a future
# edit could silently break: where objects end up, what they are called, which indexes get
# renamed, that the view is moved rather than recreated, and that the from-scratch view in
# step 5 is the same query as the one in the DDL.


def test_the_rename_script_renames_every_index_the_ddl_creates():
    """The DDL creates four named indexes; the primary key's index and the hypertable's
    time index make themselves. A new index added to the DDL without a matching entry in
    the rename list would keep its old tm_events_* name after the migration."""
    ddl = (ROOT / "sql" / "ticketmaster_events.sql").read_text()
    sql = (ROOT / "sql" / "rename_legacy.sql").read_text()
    created = set(re.findall(r"CREATE INDEX IF NOT EXISTS ticketmaster_events_(\w+)", ddl))
    assert created == {"start_idx", "city_idx", "geo_idx", "last_seen_idx"}
    listed = set(re.findall(r"'(\w+)'", re.search(r"ARRAY\[(.*?)\]", sql, re.S).group(1)))
    assert listed == created | {"pkey", "write_time_idx"}
    assert "'tm_events_' || suffix" in sql and "'ticketmaster_events_' || suffix" in sql


def test_the_rename_script_ends_up_in_geo_feeds_under_the_new_names():
    text = (ROOT / "sql" / "rename_legacy.sql").read_text()
    sql = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))   # code, not comments
    for statement in ("ALTER TABLE laddms.tm_events SET SCHEMA geo_feeds;",
                      "ALTER TABLE geo_feeds.tm_events RENAME TO ticketmaster_events;",
                      "ALTER VIEW laddms.v_tm_events_latest SET SCHEMA geo_feeds;",
                      "ALTER VIEW geo_feeds.v_tm_events_latest RENAME TO v_ticketmaster_events_latest;",
                      "CREATE SCHEMA IF NOT EXISTS geo_feeds;"):
        assert statement in sql, statement
    assert re.findall(r"SET SCHEMA (\w+)", sql) == ["geo_feeds", "geo_feeds"]   # nowhere else
    assert "ALTER INDEX geo_feeds.%I RENAME TO %I" in sql


def test_the_rename_script_moves_the_view_instead_of_recreating_it():
    """A moved view keeps its owner and its GRANTs; a dropped and recreated one loses both,
    and readers of it (dashboards, other roles) would start failing."""
    sql = (ROOT / "sql" / "rename_legacy.sql").read_text()
    code = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    assert "DROP VIEW" not in code and "DROP TABLE" not in code and "OR REPLACE" not in code
    assert code.count("CREATE VIEW") == 1                    # step 5 only: when there was none to move
    assert "ALTER VIEW laddms.v_tm_events_latest SET SCHEMA" in code


def test_the_view_the_rename_script_creates_is_the_view_in_the_ddl():
    def query(text, create):
        found = re.search(create + r"\s+AS\s+(SELECT .*?;)", text, re.S)
        assert found, create
        return re.sub(r"\s+", " ", found.group(1))

    ddl = (ROOT / "sql" / "ticketmaster_events.sql").read_text()
    sql = (ROOT / "sql" / "rename_legacy.sql").read_text()
    assert (query(ddl, r"CREATE OR REPLACE VIEW geo_feeds\.v_ticketmaster_events_latest")
            == query(sql, r"CREATE VIEW geo_feeds\.v_ticketmaster_events_latest"))


def test_the_ddl_keeps_the_key_the_chunking_and_the_index_columns():
    """What a hand edit of the DDL could change without breaking any column test: the
    service's per-cycle dedupe exists BECAUSE the primary key is (id, write_time), and
    Timescale needs the partition column in it; the chunk interval and the index columns
    are what readers' queries are tuned to."""
    ddl = (ROOT / "sql" / "ticketmaster_events.sql").read_text()
    assert "PRIMARY KEY (id, write_time)" in ddl
    assert "chunk_time_interval => INTERVAL '1 month'" in ddl
    for name, columns in {"start": "start_datetime_utc", "city": "city_name, state_code, country_code",
                          "geo": "venue_lat, venue_lon", "last_seen": "last_seen_utc"}.items():
        assert re.search(rf"ticketmaster_events_{name}_idx\s+ON geo_feeds\.ticketmaster_events \({re.escape(columns)}\);", ddl), name


def test_example_env_shows_the_same_defaults_the_code_has(monkeypatch):
    """example.env is what an operator copies: a default that differs from the code would
    silently change the topic or the table the day they remove the line."""
    shown = dict(line.split("=", 1) for line in (ROOT / "example.env").read_text().splitlines()
                 if re.match(r"[A-Z_0-9]+=", line))
    for name in ("KAFKA_TOPIC", "DB_TABLE", "KAFKA_FLUSH_TIMEOUT_S", "TM_POLL_MINS"):
        monkeypatch.delenv(name, raising=False)
    m = importlib.reload(t)
    assert shown["KAFKA_TOPIC"] == m.KAFKA_TOPIC == "nashville.ticketmaster.events"
    assert shown["DB_TABLE"] == m.DB_TABLE == "geo_feeds.ticketmaster_events"
    assert float(shown["KAFKA_FLUSH_TIMEOUT_S"]) == m.KAFKA_FLUSH_TIMEOUT_S
    assert int(shown["TM_POLL_MINS"]) == m.TM_POLL_MINS
    assert "KAFKA_TOPIC_BASENAME" not in shown        # only mentioned in a comment, as renamed


def test_the_ddl_column_types_match_what_the_service_sends():
    """The service parses the date/time columns to date/time/datetime objects, floats the
    coordinates, and relies on `name` being NOT NULL for its missing_name rule."""
    ddl = (ROOT / "sql" / "ticketmaster_events.sql").read_text()
    body = ddl.split("CREATE TABLE IF NOT EXISTS geo_feeds.ticketmaster_events (")[1].split("PRIMARY KEY")[0]
    actual = {}
    for line in body.splitlines():
        m = re.match(r"\s{4}([a-z_0-9]+)\s+(TIMESTAMPTZ|TEXT|BOOL|DATE|TIME|DOUBLE PRECISION|NUMERIC)\b(.*)", line)
        if m:
            actual[m.group(1)] = (m.group(2), "NOT NULL" in m.group(3))
    expected = {name: ("TEXT", False) for name in actual}          # everything else: nullable text
    expected.update({
        "write_time": ("TIMESTAMPTZ", True), "first_seen_utc": ("TIMESTAMPTZ", True),
        "last_seen_utc": ("TIMESTAMPTZ", True),
        "id": ("TEXT", True), "name": ("TEXT", True), "test": ("BOOL", True),
        "start_local_date": ("DATE", False), "start_local_time": ("TIME", False),
        "start_datetime_utc": ("TIMESTAMPTZ", False), "onsale_start_utc": ("TIMESTAMPTZ", False),
        "onsale_end_utc": ("TIMESTAMPTZ", False),
        "venue_lat": ("DOUBLE PRECISION", False), "venue_lon": ("DOUBLE PRECISION", False),
        "price_min": ("NUMERIC", False), "price_max": ("NUMERIC", False),
    })
    assert len(actual) == 37
    assert actual == expected


# ----- other repo files that must stay in step with the code -------------------------


def test_dockerignore_keeps_secrets_nested_git_dirs_and_tests_out_of_the_image():
    """`**/.git` matters most: the Drone clone step puts tokenised .git/config files in the
    build context, and without it `COPY . .` would ship them in the pushed image."""
    lines = {l.strip() for l in (ROOT / ".dockerignore").read_text().splitlines()
             if l.strip() and not l.startswith("#")}
    assert {"**/.git", ".git", "*.env", ".env", "tests", ".pytest_cache"} <= lines


def test_requirements_list_only_what_the_service_imports():
    """numpy, pytz and filelock sat in requirements.txt without ever being used (numpy was
    imported once and never called); every one of them is a dependency to install, scan
    and keep patched in the image. tzdata is the one deliberate exception: zoneinfo needs
    a tz database but never imports the package."""
    requirements = [re.split(r"[=<>~! ]", l.strip(), maxsplit=1)[0]
                    for l in (ROOT / "requirements.txt").read_text().splitlines()
                    if l.strip() and not l.startswith(("#", "./"))]
    tree = ast.parse((ROOT / "ticketmaster2kafka.py").read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    pip_to_import = {"python-dotenv": "dotenv"}
    unused = [r for r in requirements if r != "tzdata" and pip_to_import.get(r, r) not in imported]
    assert requirements and unused == []


def test_the_readme_lists_every_log_event_the_code_emits_at_its_level():
    code = (ROOT / "ticketmaster2kafka.py").read_text()
    in_code = re.findall(r'\b_?log\.(debug|info|warning|error|critical)\(\s*"([a-z_]+)"', code)
    levels_in_code = {name: level.upper() for level, name in in_code}
    assert len(set(in_code)) == len(levels_in_code), "an event is logged at two different levels"
    section = (ROOT / "README.md").read_text().split("\n## Logs\n")[1].split("\n## ")[0]
    levels_in_readme = {name: level for name, level in re.findall(r"(?m)^\| `([a-z_]+)` \| ([A-Z]+) \|", section)}
    assert levels_in_code and levels_in_readme == levels_in_code


def test_the_readme_lists_every_metric_the_code_registers():
    """The alert rules and dashboards are written from this table, so a metric that is
    registered but undocumented (or documented but gone) is a silent hole."""
    code = (ROOT / "ticketmaster2kafka.py").read_text()
    in_code = set(re.findall(r'\btel\.(?:counter|gauge|histogram)\(\s*"([a-z_]+)"', code))
    section = (ROOT / "README.md").read_text().split("\n## Metrics\n")[1].split("\n## ")[0]
    in_readme = set(re.findall(r"(?m)^\| `([a-z_]+)(?:\{[a-z_, ]+\})?` \|", section))
    assert {"events_fetched_total", "db_write_errors_total", "fetch_seconds"} <= in_code   # the regex finds them
    assert in_readme == in_code


def test_every_env_var_the_code_reads_is_in_the_readme_and_example_env():
    code = (ROOT / "ticketmaster2kafka.py").read_text()
    read = set(re.findall(r"""(?:getenv|environ\.get|environ)\(?\[?\s*["']([A-Z][A-Z0-9_]+)["']""", code))
    assert {"KAFKA_TOPIC", "DB_TABLE", "KAFKA_FLUSH_TIMEOUT_S", "TICKETMASTER_API_KEY", "TM_WINDOW_DAYS"} <= read
    section = (ROOT / "README.md").read_text().split("\n## Configuration\n")[1].split("\n## ")[0]
    documented = {name for row in section.splitlines() if row.startswith("| `")
                  for name in re.findall(r"`([A-Z][A-Z0-9_]+)`", row.split("|")[1])}
    example = (ROOT / "example.env").read_text()
    # KAFKA_TOPIC_BASENAME is only described in prose and a comment: it is ignored (and warned about)
    for name in sorted(read - {"KAFKA_TOPIC_BASENAME"}):
        assert name in documented, f"{name} is read by the code but missing from the README's variable table"
        assert re.search(rf"(?m)^#?\s*{name}=", example), f"{name} is missing from example.env"
