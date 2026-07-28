# ticketmaster2kafka

Pages the Ticketmaster Discovery API for events in the configured geo window,
produces one message per event to Kafka, and writes a flattened row per event
to the `laddms.tm_events` TimescaleDB hypertable (schema in
`ticketmaster_tables.sql`).

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
| `KAFKA_TOPIC_BASENAME` | Topic to produce to, default `nashville-tm` |
| `DB_HOST`, `DB_PORT`, `DB_DBNAME`, `DB_USER`, `DB_PASSWORD` | Postgres/TimescaleDB target (`NDOT`) |
| `DB_TABLE` | Default `laddms.tm_events` |
| `SERVICE_NAME` | Telemetry service name, default `ticketmaster2kafka` |
| `TM_BASE_URL`, `HTTP_TIMEOUT_S` | Upstream base URL and HTTP timeout |
| `TM_POLL_MINS` | Poll cadence in minutes, default `60` |
| `TICKETMASTER_API_KEY` | Required |
| `TM_PAGE_SIZE`, `TM_COUNTRY_CODE`, `TM_WINDOW_DAYS` | Discovery paging and window |
| `TM_START_ISO`, `TM_END_ISO` | Absolute bounds; win over `TM_WINDOW_DAYS` |
| `TM_EXTRA_PARAMS` | Extra query params, e.g. `classificationName=music,city=Nashville` |
| `TELEMETRY_LOG_LEVEL` | `DEBUG` for debug logging (default `INFO`) |

Renamed in the 1.0 migration: `SQL_HOSTNAME`/`SQL_PORT`/`SQL_USERNAME`/
`SQL_PASSWORD` → `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`, plus a new
`DB_DBNAME` (was hardcoded to `NDOT`), and `LOG_PATH`/`DEBUG` →
`TELEMETRY_LOG_LEVEL` (logs go to stdout, no log file). The deployment's
Secret/ConfigMap must supply the new names.

The Strimzi CA cert must live on the host at `/etc/viewlive/certs/strimzi-ca.crt`
and is mounted read-only into the container. Override with `KAFKA_CA_LOCATION`
if you need a different in-container path, or supply the CA inline with
`KAFKA_CA_CERT` and skip the mount entirely.

### Docker

The `lv_*` connectors are private, so the build needs a GitHub token. The
builder stage installs them from their default branches and
`requirements.txt` carries lower bounds only (`>=1.0`), so connector fixes
arrive on the next image build:
```
docker build --build-arg GITHUB_TOKEN=$GITHUB_TOKEN -t ticketmaster2kafka:0.0 .
docker run \
  --env-file path/to/1.env \
  -v /etc/viewlive/certs:/etc/viewlive/certs:ro \
  -p 9100:9100 \
  ticketmaster2kafka:0.0
```

### CI/CD

Drone (`.drone.yml`) runs on every push to `main`: it builds the image, pushes
`docker.mogi.io/ticketmaster2kafka:<commit-sha>`, then bumps the image tag in
`2kafka/ticketmaster/deployment.yaml` of `k8s-manifests-backend` to deploy.
