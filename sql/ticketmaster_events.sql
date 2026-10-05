-- Schema for ticketmaster2kafka. Apply once before running the service:
--
--   psql -h $DB_HOST -U $DB_USER -d $DB_DBNAME -f sql/ticketmaster_events.sql
--
-- Idempotent: every statement is IF NOT EXISTS / OR REPLACE, so re-running it is safe.
--
-- If the OLD table (laddms.tm_events) already exists on the target database, do NOT run
-- this file first: run sql/rename_legacy.sql instead, which moves the existing table,
-- its history and its view to the names below. (Running this file first would create a
-- second, empty table next to the old one.)
--
-- Ensure TimescaleDB is installed in the DB first:
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS geo_feeds;

-- One row per event per poll (an append-only history of snapshots, never an upsert).
CREATE TABLE IF NOT EXISTS geo_feeds.ticketmaster_events (
    -- hypertable partition column (must be NOT NULL)
    write_time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- identity (not unique alone on hypertable; see composite PK below)
    id                    TEXT NOT NULL,

    name                  TEXT NOT NULL,
    url                   TEXT,
    source                TEXT,
    locale                TEXT,
    test                  BOOL NOT NULL DEFAULT FALSE,

    status_code           TEXT,
    timezone              TEXT,
    start_local_date      DATE,
    start_local_time      TIME,
    start_datetime_utc    TIMESTAMPTZ,
    onsale_start_utc      TIMESTAMPTZ,
    onsale_end_utc        TIMESTAMPTZ,

    venue_id              TEXT,
    venue_name            TEXT,
    venue_address_line1   TEXT,
    city_name             TEXT,
    state_code            TEXT,
    country_code          TEXT,
    venue_postal_code     TEXT,
    venue_timezone        TEXT,
    venue_lat             DOUBLE PRECISION,
    venue_lon             DOUBLE PRECISION,

    attraction_primary    TEXT,
    attraction_names      TEXT,
    class_segment         TEXT,
    class_genre           TEXT,
    class_subgenre        TEXT,
    class_type            TEXT,
    class_subtype         TEXT,
    image_url_primary     TEXT,
    price_currency        TEXT,
    price_min             NUMERIC,
    price_max             NUMERIC,

    first_seen_utc        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_utc         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Timescale requires the partition column in any unique/PK constraint
    PRIMARY KEY (id, write_time)
);

-- Indexes
CREATE INDEX IF NOT EXISTS ticketmaster_events_start_idx
    ON geo_feeds.ticketmaster_events (start_datetime_utc);

CREATE INDEX IF NOT EXISTS ticketmaster_events_city_idx
    ON geo_feeds.ticketmaster_events (city_name, state_code, country_code);

CREATE INDEX IF NOT EXISTS ticketmaster_events_geo_idx
    ON geo_feeds.ticketmaster_events (venue_lat, venue_lon);

CREATE INDEX IF NOT EXISTS ticketmaster_events_last_seen_idx
    ON geo_feeds.ticketmaster_events (last_seen_utc);

-- Your column comments (unchanged)
COMMENT ON COLUMN geo_feeds.ticketmaster_events.name IS 'Event name in Ticketmaster.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.url IS 'Event URL from Ticketmaster.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.start_datetime_utc IS 'Event start date/time.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.onsale_start_utc IS 'Ticket sale start at date/time.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.onsale_end_utc IS 'Ticket sales stop at date/time.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.venue_name IS 'Venue name in Ticketmaster.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.venue_address_line1 IS 'Venue street address in Ticketmaster.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.venue_lat IS 'Venue latitude reported by Ticketmaster.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.venue_lon IS 'Venue longitude reported by Ticketmaster.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.first_seen_utc IS 'First time the event was seen in our database.';
COMMENT ON COLUMN geo_feeds.ticketmaster_events.last_seen_utc IS 'Last time the event was seen in our database.';

-- Convert to hypertable (monthly chunks is a good default)
SELECT create_hypertable(
  'geo_feeds.ticketmaster_events',
  'write_time',
  if_not_exists => TRUE,
  chunk_time_interval => INTERVAL '1 month'
);

-- Optional: "latest snapshot" view for easy querying
CREATE OR REPLACE VIEW geo_feeds.v_ticketmaster_events_latest AS
SELECT DISTINCT ON (id)
  id, write_time, name, url, source, locale, test,
  status_code, timezone, start_local_date, start_local_time, start_datetime_utc,
  onsale_start_utc, onsale_end_utc,
  venue_id, venue_name, venue_address_line1, city_name, state_code, country_code,
  venue_postal_code, venue_timezone, venue_lat, venue_lon,
  attraction_primary, attraction_names,
  class_segment, class_genre, class_subgenre, class_type, class_subtype,
  image_url_primary, price_currency, price_min, price_max,
  first_seen_utc, last_seen_utc
FROM geo_feeds.ticketmaster_events
ORDER BY id, last_seen_utc DESC;

-- Grants for the role the service connects as (DB_USER). The service only INSERTs
-- (executemany below 500 rows, COPY at 500 and above; both need just INSERT). Without
-- these an INSERT fails with "permission denied" and shows up as
-- ticketmaster2kafka_db_write_errors_total. Uncomment and fill in the role name.
-- GRANT USAGE ON SCHEMA geo_feeds TO <app_role>;
-- GRANT INSERT ON geo_feeds.ticketmaster_events TO <app_role>;
-- Readers of the table and the view need SELECT (and USAGE on the schema):
-- GRANT SELECT ON geo_feeds.ticketmaster_events, geo_feeds.v_ticketmaster_events_latest TO <reader_role>;

-- Optional: compression & retention policies (uncomment/tune as desired)
-- ALTER TABLE geo_feeds.ticketmaster_events
--   SET (timescaledb.compress, timescaledb.compress_segmentby = 'id');
-- SELECT add_compression_policy('geo_feeds.ticketmaster_events', INTERVAL '7 days');
-- SELECT add_retention_policy('geo_feeds.ticketmaster_events',  INTERVAL '90 days');
