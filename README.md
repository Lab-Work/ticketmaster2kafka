# ticketmaster2kafka

Pages the Ticketmaster Discovery API for events in the configured geo window,
produces one message per event to Kafka (`nashville.ticketmaster.events`), and
writes a flattened row per event to the `geo_feeds.ticketmaster_events`
TimescaleDB hypertable (schema in `sql/ticketmaster_events.sql`).

Logs are JSON on stdout; Prometheus metrics are on `:9100/metrics`.

## Configuration

Copy `example.env` to `.env` (or pass it with `--env-file`) and fill in the
credentials. The variables:

| Variable | Notes |
|---|---|
| `KAFKA_BOOTSTRAP`, `KAFKA_USER`, `KAFKA_PASSWORD` | Broker and SASL credentials |
| `KAFKA_CA_LOCATION` | Path to the Strimzi CA cert in the container |
| `KAFKA_CA_CERT` | Optional inline PEM CA; takes precedence over `KAFKA_CA_LOCATION` |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM` | Default `SASL_SSL` / `SCRAM-SHA-512` |
| `KAFKA_TOPIC` | Topic to produce to, default `nashville.ticketmaster.events` |
| `KAFKA_FLUSH_TIMEOUT_S` | Seconds to wait at the end of each batch for broker acknowledgement, default `10` |
| `DB_HOST`, `DB_PORT`, `DB_DBNAME`, `DB_USER`, `DB_PASSWORD` | Postgres/TimescaleDB target (`NDOT`) |
| `DB_TABLE` | Default `geo_feeds.ticketmaster_events` |
| `SERVICE_NAME` | Telemetry service name, default `ticketmaster2kafka` |
| `TM_BASE_URL`, `HTTP_TIMEOUT_S` | Upstream base URL and HTTP timeout |
| `TM_POLL_MINS` | Poll cadence in minutes, default `60` |
| `TICKETMASTER_API_KEY` | Required |
| `TM_PAGE_SIZE`, `TM_COUNTRY_CODE`, `TM_WINDOW_DAYS` | Discovery paging and window |
| `TM_START_ISO`, `TM_END_ISO` | Absolute bounds; pin the window and win over `TM_WINDOW_DAYS` |
| `TM_EXTRA_PARAMS` | Extra query params, e.g. `classificationName=music,city=Nashville` |
| `TELEMETRY_LOG_LEVEL` | `DEBUG` for debug logging (default `INFO`) |

Renamed in the 1.0 migration: `SQL_HOSTNAME`/`SQL_PORT`/`SQL_USERNAME`/
`SQL_PASSWORD` → `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`, plus a new
`DB_DBNAME` (was hardcoded to `NDOT`), and `LOG_PATH`/`DEBUG` →
`TELEMETRY_LOG_LEVEL` (logs go to stdout, no log file). The deployment's
Secret/ConfigMap must supply the new names.

Renamed with the topic/table rename (2026-10-04):

- `KAFKA_TOPIC_BASENAME` → **`KAFKA_TOPIC`**, and its default changed from
  `nashville-tm` to `nashville.ticketmaster.events`. (The old name was
  misleading: it always held a full topic name.) A deployment that still sets
  `KAFKA_TOPIC_BASENAME` has it **ignored** — the service publishes to the
  default topic instead — and logs a `legacy_env_ignored` WARNING at startup.
  The manifest must set `KAFKA_TOPIC` (or drop the old variable); see
  [Rolling out the rename](#rolling-out-the-rename-order-matters) for the order.
- `DB_TABLE` keeps its name but its default changed from `laddms.tm_events` to
  `geo_feeds.ticketmaster_events`; the view is now
  `geo_feeds.v_ticketmaster_events_latest` (was `laddms.v_tm_events_latest`) and
  the indexes are `ticketmaster_events_*` (were `tm_events_*`). **A deployment that
  sets `DB_TABLE=laddms.tm_events` explicitly overrides the new default** and must
  be changed to the new name (or have the variable removed), in the same step
  as the database rename.
- New: `KAFKA_FLUSH_TIMEOUT_S`.

`KAFKA_CA_LOCATION` defaults to `strimzi-ca.crt`, resolved relative to the
container's `/app` working directory — hence the `-v` below mounting the cert
there read-only. Set `KAFKA_CA_LOCATION` to an absolute path if the cert lives
elsewhere (a deployment mounting `/etc/viewlive/certs` would do that), or skip
the mount entirely by supplying `KAFKA_CA_CERT` as an inline PEM string, which
takes precedence.

## Database

Two scripts in `sql/`, both applied by hand with `psql` (the service never
creates or alters anything):

```
psql -h $DB_HOST -U $DB_USER -d $DB_DBNAME -f sql/ticketmaster_events.sql   # first creation
psql -h $DB_HOST -U $DB_USER -d $DB_DBNAME -f sql/rename_legacy.sql         # OR: move the old table
```

- `sql/ticketmaster_events.sql` is idempotent and creates the schema, the
  hypertable, its indexes and the latest-snapshot view. It ends with a commented
  `GRANT` block for the role in `DB_USER`: the service needs `USAGE` on
  `geo_feeds` and `INSERT` on the table, and without them every insert fails
  (visible as `db_write_errors_total`).
- `sql/rename_legacy.sql` is for a database where the **old** `laddms.tm_events`
  already exists. It moves the table (and so its history) to
  `geo_feeds.ticketmaster_events`, renames its indexes, and moves the view to
  `geo_feeds.v_ticketmaster_events_latest`. Each step is guarded (the old object
  must exist and the new one must not), so it is safe to re-run and a second run
  does nothing. Run it **instead of** the first script, before the new image
  writes, and back to back with the rollout (see
  [Rolling out the rename](#rolling-out-the-rename-order-matters)); if both tables
  already exist it refuses and tells you to decide by hand.

The table is an append-only history: one row per event per poll, primary key
`(id, write_time)`. It is never upserted, so `first_seen_utc` and
`last_seen_utc` equal `write_time` on every row.

## Rolling out the rename (order matters)

The topic and the table both changed name, and the deployment manifest
(`2kafka/ticketmaster/deployment.yaml` in `k8s-manifests-backend`, outside this
repo) sets `KAFKA_TOPIC_BASENAME` and `DB_TABLE` explicitly. The new image
**ignores** the first and **obeys** the second, so the manifest, the database and
the broker have to move together. Do it in this order:

1. **Broker.** Create the topic `nashville.ticketmaster.events` and the producer
   ACLs for the service's Kafka user, and tell the consumers of `nashville-tm`
   they must switch topics (nothing will keep writing to the old one).
2. **Manifest, prepared but not applied yet.** `KAFKA_TOPIC=nashville.ticketmaster.events`
   (or drop the old variable; the default is the same) and
   `DB_TABLE=geo_feeds.ticketmaster_events` (or drop the variable), in the same
   change as the image tag bump. A manifest that keeps `DB_TABLE=laddms.tm_events`
   writes to the old name no matter what the image defaults to.
3. **Database, then rollout, back to back.** Run `sql/rename_legacy.sql` (or
   `sql/ticketmaster_events.sql` on a database that never had the old table),
   make sure the service's role has `USAGE` on `geo_feeds` and `INSERT` on the
   table (moving a table does not grant schema access), then apply the manifest
   and the new image together. A push to `main` is itself the rollout (Drone
   builds the image and bumps the tag, see CI/CD), so merge only when steps 1 and
   2 are done and you are ready to run the script. Whichever of the new manifest
   or the new image reaches the cluster first, the table must **already exist
   under the new name** at that moment, because both point the service at it.
   And the old pod must not be left running for long after the rename: it still
   writes to `laddms.tm_events`, which no longer exists.

What each mistake looks like, because most of them do not crash anything:

| What went wrong | What you see |
|---|---|
| The new image starts before the table is renamed or created, or the manifest still has `DB_TABLE=laddms.tm_events` after the rename | Every insert fails with `relation "..." does not exist`. Per poll: two `db_insert_failed` ERROR lines, `db_write_errors_total` +2 and `db_rows_dropped_total` +N. The service does not crash or restart and Kafka is unaffected, so nothing else flags it. That poll's snapshot is lost for good (a poll is the API as it was at that moment). Once the table exists the next poll writes normally with no restart; a wrong `DB_TABLE` needs the manifest fixed and the pod restarted. |
| `rename_legacy.sql` has run but the old pod is still running | The same failure from the OLD image, which logs `Failed to insert Ticketmaster events` once per poll and drops the rows, until the new image replaces it. Do not leave a gap between the rename and the rollout. |
| The service's role lacks `USAGE` on `geo_feeds` or `INSERT` on the table | `permission denied for schema geo_feeds` (or `for table ticketmaster_events`) in `db_insert_failed`; the same counters as above. |
| The manifest still sets `KAFKA_TOPIC_BASENAME` (and not `KAFKA_TOPIC`) | The variable is ignored: a `legacy_env_ignored` WARNING at startup, and the service publishes to the default `nashville.ticketmaster.events`, not `nashville-tm`. Consumers of `nashville-tm` quietly stop receiving events. |
| The new topic does not exist yet (auto-create off) or the user has no ACL on it | `produce()` accepts everything, the broker refuses it: `kafka_delivery_errors_total{reason="UNKNOWN_TOPIC_OR_PART"}` (or `TOPIC_AUTHORIZATION_FAILED`) rises while `events_delivered_total` stays flat, and `kafka_delivery_failed` is logged once a minute. The database keeps writing. |

## Behaviour

**Query window.** Unless `TM_START_ISO` / `TM_END_ISO` are set, each poll asks
for events starting between *now* and *now + `TM_WINDOW_DAYS`*, recomputed from
the clock on every poll, so a long-running pod never goes blind. Each poll logs
`fetch_window` with the bounds it used, and a poll that returns no events logs a
`fetch_returned_no_events` WARNING. When `TM_START_ISO` / `TM_END_ISO` are set
they are sent as given on every poll.

**Upstream failures.** A request that hits a 429, a 5xx, a timeout or a
connection error is retried, up to 3 attempts per page, sleeping about 2 s then
4 s (exponential, with jitter). Other 4xx answers (a bad key, a bad query) and
unreadable 200s are not retried. If the last attempt fails the poll is
abandoned (`fetch_cycle_abandoned`) and the service waits for the next one;
nothing partial is ever written. SIGTERM is honoured between attempts and
between pages.

**The API key never reaches a log.** It is sent as `?apikey=` and the text of
every `requests` exception embeds the URL, so failures are logged only as host,
path (no query string), status code and exception class. urllib3's logger is
silenced, and the handlers for unexpected exceptions mask `apikey=` values.

**Database writes.** The connector inserts a whole batch in one transaction, so
a single row the database refuses would lose the whole poll. Three things
prevent that:

1. *Row building.* Events with no `id` or no `name`, or that cannot be
   flattened, are skipped and counted (`events_invalid_total`,
   `invalid_events_skipped` WARNING). Repeated ids are collapsed, keeping the
   last (`duplicates_skipped_total`). NUL characters (and lone surrogates, which
   UTF-8 cannot encode) are removed from text, and a date or time that does not
   parse (`TBD`) becomes NULL; an `event_fields_sanitized` WARNING lists a few
   ids and fields.
2. *One retry.* A failed insert is retried once; the pool hands out a fresh
   connection, which is what a stale idle one needs.
3. *Row by row.* If the database refuses the data itself (SQLSTATE class 22,
   a data exception; class 23, an integrity violation; or 42804, a datatype
   mismatch such as a list where a column wants a number) the rows are
   inserted one at a time, so only the rows it refuses are lost. This costs one
   insert (and commit) per row and only happens on a cycle that has such a row:
   against a local database a 600-row cycle with a few bad rows took about 0.9 s,
   and a cycle where the database refuses every row (schema drift) took 7.6 s on a
   freshly created table. A remote database adds a round trip per row, and a poll
   is never more than 1000 events. The pass is not interrupted by SIGTERM.

A failure that is not about one row (a lost connection, a permission, a missing
table) is *not* tried row by row: the rows would all fail the same way.

Every failure is both **counted and logged at ERROR**, so an alert on the
counters and a search of the logs always agree, and the service carries on with
the next poll:

- each failed insert of the whole poll (the first attempt, the retry, or a batch
  the database refused) is one `db_write_errors_total` and one `db_insert_failed`
  ERROR with the exception class and message;
- each row given up on is a `db_rows_dropped_total`: all of them after the retry
  also fails, or each row the database refused in the row-by-row pass. The
  refused ones are also in `events_invalid_total{reason="db_rejected"}`, which
  tells a data problem from a connection problem, and one `db_rows_rejected` ERROR
  lists a few of them with their error class and the database's reason.

Kafka always receives every fetched event, whatever happens to the database.

**Kafka.** The message is unchanged: key `0`, headers `service=ticketmaster` and
`datatype=event`, and a value that is a JSON *string* holding the JSON payload
(`{"source", "fetched_at", "event"}`); external consumers depend on that.
`events_emitted_total` counts messages *enqueued*; the broker's verdict arrives
later in a per-message callback, which is served by the flush at the end of every
batch and by a poll every second while the service waits.

## Metrics

All names carry the `ticketmaster2kafka_` prefix. What to alert on is in bold.

| Metric | Meaning |
|---|---|
| `events_fetched_total` | Events fetched from the API |
| `fetch_seconds` | Histogram of the paging run |
| `fetch_errors_total{reason}` | **Failed upstream attempts**; reason is `timeout`, `connection`, `http_429`, `http_4xx`, `http_5xx` or `invalid_response` |
| `last_fetch_success_timestamp_seconds` | **Unix time of the last fully fetched poll** (alert on `time() - this`) |
| `events_emitted_total` | Events enqueued to Kafka |
| `events_delivered_total` | Messages the broker acknowledged |
| `kafka_delivery_errors_total{reason}` | **Messages Kafka refused or never acknowledged**; reason is the librdkafka error name (`UNKNOWN_TOPIC_OR_PART`, `TOPIC_AUTHORIZATION_FAILED`, `_MSG_TIMED_OUT`, ...), `enqueue_error` (never left this process) or `poll_error` / `flush_error` (the producer is in a fatal state) |
| `events_invalid_total{reason}` | Events that never reached the database: `missing_id`, `missing_name`, `malformed` (skipped before the insert) or `db_rejected` (the database refused the row; the rest of the cycle was inserted) |
| `duplicates_skipped_total` | Repeated ids collapsed before the insert |
| `db_write_errors_total` | **Failed inserts**, whatever the cause: a lost connection, a permission, a missing table, or the database refusing the data. One per failed attempt of the whole poll (the first, the retry), plus one if the row-by-row pass hits a failure that is not about the row, so a single poll adds at most 3; each has a `db_insert_failed` ERROR. One bad row adds 1 |
| `db_rows_dropped_total` | **Rows given up on**: all of a poll's rows after the retry also failed, or each row the database refused one by one |

`db_rows_dropped_total` rising by one or two now and then (with
`events_invalid_total{reason="db_rejected"}` rising by the same amount) is
Ticketmaster data the database does not like. Rising by about the number of rows in
a poll means the table and the service disagree about the schema (a new NOT NULL
column, a changed type): the whole sink is dead while Kafka keeps flowing, and
`db_rows_rejected` shows `inserted: 0`.

`events_emitted_total` rising while `events_delivered_total` stays flat means
Kafka is refusing the messages; `kafka_delivery_errors_total` says why. A producer
in a fatal state (for example a missing cluster-level permission for idempotent
writes) cannot recover by itself: the service keeps running, and keeps writing the
database, but every message fails until the pod is restarted. (There is
no `db_queue_depth` gauge: the insert is synchronous, nothing queues in front of
the database.)

## Logs

Every log line's message is a stable event name with the details as JSON fields.
This is the complete list of what the service itself emits (a test keeps it in
step with the code):

| Event | Level | Fields and meaning |
|---|---|---|
| `connectors` | INFO | `connectors`: the version of every `lv_*` connector in the process |
| `startup` | INFO | `service`, `upstream`, `poll_interval_mins`, `topic`, `db_table` |
| `legacy_env_ignored` | WARNING | `env_var`, `replaced_by`, `topic_in_use`: `KAFKA_TOPIC_BASENAME` is still set and is ignored |
| `worker_starting` | INFO | The worker thread is being started |
| `producer_created` | INFO | The feed object is built |
| `fetch_window` | INFO | `start`, `end`, `fixed`: the query window of this poll |
| `fetch_complete` | INFO | `events`, `pages`, plus the window: every page was fetched |
| `fetch_returned_no_events` | WARNING | The window: a successful poll that returned nothing |
| `upstream_fetch_failed` | ERROR | `upstream`, `path`, `status_code`, `error_class`, `reason`, `attempt`, `max_attempts`, `page`, `retry_in_s`: one failed request attempt (never the URL's query string) |
| `fetch_cycle_abandoned` | WARNING | `reason`, `page`, `events_so_far`: the poll is given up, nothing is written |
| `fetch_cycle_failed` | ERROR | `error_class`, `traceback`: an unexpected exception while fetching (key masked) |
| `first_event` | DEBUG | `event`, `flattened`: the first event of each poll, only at `TELEMETRY_LOG_LEVEL=DEBUG` |
| `kafka_batch_enqueued` | INFO | `topic`, `enqueued`, `enqueue_failed` |
| `kafka_delivery_failed` | ERROR | `topic`, `reason`, `error`, `partition`, `suppressed`; at most one line per reason per minute |
| `kafka_flush_incomplete` | WARNING | `queued`: messages still unacknowledged when the end-of-batch flush timed out |
| `kafka_batch_failed` | ERROR | `error_class`, `traceback`: an unexpected exception in the Kafka stage (key masked) |
| `invalid_events_skipped` | WARNING | `count`, `by_reason`, `sample`: events left out of the database insert |
| `event_fields_sanitized` | WARNING | `count`, `sample` (`id`, `field`, `fix`): NUL or unencodable text cleaned, or an unparseable date or time set to NULL |
| `duplicate_events_skipped` | INFO | `count`: repeated ids collapsed |
| `db_insert_skipped` | INFO | `reason`, `events`: nothing valid to insert |
| `db_rows_inserted` | INFO | `table`, `rows`, `events`, `duplicates_skipped`, `invalid_skipped`, `rejected` |
| `db_rows_rejected` | ERROR | `table`, `count`, `rows`, `inserted`, `error_class`, `batch_error`, `sample` (`id`, `error_class`, `error`): the database refused these rows (given up on, counted in `db_rows_dropped_total`) and took the other `inserted` |
| `db_insert_failed` | ERROR | `table`, `rows`, `attempt`, `will_retry`, `will_isolate_rows`, `row_by_row`, `error_class`, `error`: an insert failed, whatever the cause (one per `db_write_errors_total`). `will_isolate_rows` is true when the database refused the data and the rows are about to be tried one by one |
| `db_batch_failed` | ERROR | `error_class`, `traceback`: an unexpected exception in the database stage (key masked) |
| `worker_thread_crashed` | CRITICAL | `worker`, `error_class`, `traceback`: the worker died; the process exits non-zero |
| `shutdown` | INFO | The process is exiting |

The `lv_*` connectors log under their own logger names (for example
lv_db_connector's `connection lost; retrying once`); those lines are not listed here.

## Known limits

Understood, and deliberately not fixed in this service.

**A silently dropped database connection hangs the insert.** Neither this
service nor `lv_db_connector` puts a timeout on the database socket. If the
connection is black-holed (a frozen database host, a firewall that drops an idle
connection without resetting it) the insert does not return until the operating
system gives up on the TCP connection, which can take many minutes: in tests
against a paused database container the insert was still blocked after 40 s, and
after 3 minutes in a longer test. While it is blocked there is **no log line and
no counter** (`db_write_errors_total` stays at 0) and the poll loop is stalled; the
only signal is `last_fetch_success_timestamp_seconds` going stale, so alert on
that. A SIGTERM in that state took 30 to 40 s to finish (30 s waiting for the
worker, then closing the connections; measured 30 s and 40 s), which is at least
Kubernetes' default 30 s grace period, so the pod is SIGKILLed. A connection that
is *reset* (for example the backend is terminated) is fine: the insert fails at
once and the retry reconnects. The fix belongs in `lv_db_connector` (TCP keepalive
and `tcp_user_timeout` on its connections), where every service would get it.

**A request in flight delays shutdown.** SIGTERM sets a flag that is checked
between request attempts and between pages, not during a request. If it arrives
while a request to Ticketmaster is in flight the process waits for it: up to
`HTTP_TIMEOUT_S` (default 30 s, applied by `requests` to the connect and to each
read separately, not as one deadline). `main()` stops waiting for the worker after
30 s and the worker is a daemon thread, so it cannot hold the process past that;
closing Kafka can then wait up to 10 s more for queued messages. So the worst case
is on the order of 40 s, against a grace period of 30 s that Kubernetes applies
unless the manifest sets another (it did not when this was written). Measured: 21 s
after a SIGTERM that arrived 4 s into an upstream that stalled for 25 s, and 19 s with
an unreachable broker (the end-of-batch flush, then closing Kafka). A
SIGKILL at that point costs at most the poll in progress: the database insert is
one transaction (all of it or none) and the restarted pod polls again at once.
Lowering `HTTP_TIMEOUT_S` lowers the worst case. The row-by-row insert pass
(see Database writes, at most 1000 inserts) is not interrupted by SIGTERM either;
it too is bounded by the 30 s `main()` waits for the worker.

## Docker

The `lv_*` connectors are private Lab-Work repos, and the Dockerfile does not
download them: it `COPY`s `./lv_telemetry_connector`, `./lv_kafka_connector` and
`./lv_db_connector` from the build context and `requirements.txt` installs them
from those paths. In CI, Drone's `clone-private-repo` step puts the three clones
there; for a local build, clone them into the repo directory yourself first
(their `.git` directories are kept out of the image by `.dockerignore`). They are
not version-pinned: an image gets whatever each connector's default branch held
when it was cloned (the startup `connectors` log line says which versions). The
`GITHUB_TOKEN` build arg is vestigial and unused:
```
docker build -t ticketmaster2kafka:0.0 .
docker run \
  --env-file path/to/1.env \
  -v $(pwd)/strimzi-ca.crt:/app/strimzi-ca.crt:ro \
  -p 9100:9100 \
  ticketmaster2kafka:0.0
```

## CI/CD

Drone (`.drone.yml`) runs on every push to `main`: it builds the image, pushes
`docker.mogi.io/ticketmaster2kafka:<commit-sha>`, then bumps the image tag in
`2kafka/ticketmaster/deployment.yaml` of `k8s-manifests-backend` to deploy.

## Tests

```
pip install -e ../lv_telemetry_connector -e ../lv_kafka_connector -e ../lv_db_connector pytest
python3 -m pytest tests
```

No Docker, Kafka, Postgres or internet is needed (the `lv_*` connectors only have to
be importable): Kafka, the database, the clock and the upstream are fakes, and
`tests/conftest.py` clears the proxy variables (so a `HTTP_PROXY` in the shell
cannot divert them) and refuses any connection that is not to loopback. Loopback
itself is used: 20 tests start a throwaway HTTP server on `127.0.0.1` (ephemeral
port) or bind and close a loopback port, so an environment with loopback blocked
fails those. The suite does not exercise a real broker or TimescaleDB; the SQL
scripts and the insert paths (executemany below 500 rows, COPY at 500 and above)
were checked by hand against throwaway local containers.
