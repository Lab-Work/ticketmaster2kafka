-- One-time, hand-run migration: laddms.tm_events -> geo_feeds.ticketmaster_events
--
-- WHEN TO RUN IT
--   Only on a database where the OLD objects already exist (the pre-rename service wrote
--   laddms.tm_events, and laddms.v_tm_events_latest was created next to it). It keeps the
--   existing hypertable, every row of history and the chunk layout, and just gives them
--   the new names. If the old table is NOT there, there is nothing to do here: run
--   sql/ticketmaster_events.sql instead, which creates everything fresh.
--
-- HOW
--   Run it BEFORE the new image starts writing (the service never creates or renames
--   anything itself), connected as the table's owner or a superuser:
--
--     psql -h $DB_HOST -U $DB_USER -d $DB_DBNAME -f sql/rename_legacy.sql
--
--   Optionally add -1 to run it as a single transaction. Without it each step below is its
--   own transaction, which is fine: every step is guarded.
--
--   Re-runnable: each step runs only if the old object exists AND the new one does not,
--   and otherwise prints a NOTICE saying why it did nothing. A second run is a no-op.
--
-- ORDER OF OPERATIONS (read this before running it on production)
--   The service writes to whatever DB_TABLE names, and the deployment manifest sets DB_TABLE
--   (and KAFKA_TOPIC_BASENAME, which the new image ignores) explicitly. So the rename and
--   the rollout have to happen together, back to back:
--     1. Get the manifest change ready: DB_TABLE=geo_feeds.ticketmaster_events (or the
--        variable removed), KAFKA_TOPIC=nashville.ticketmaster.events (or the old variable
--        removed), the new image tag. The new topic and its ACLs must exist on the broker.
--     2. Run this script.
--     3. Roll the new image out immediately. (A push to main IS the rollout: Drone builds
--        the image and bumps its tag in the manifests repo, README.md "CI/CD". So merge
--        only once steps 1 and 2 are done and you are about to run this script.)
--   Before step 2 the OLD image keeps working as it always did. Between steps 2 and 3 the
--   old pod (DB_TABLE=laddms.tm_events) fails every insert with "relation does not exist"
--   and drops that poll's rows, so keep that window short.
--   The reverse mistake is just as bad: the NEW image started before this script has run,
--   or with DB_TABLE=laddms.tm_events still in the manifest after it has, finds no such
--   table and fails every insert the same way. The service does not crash; only
--   db_write_errors_total and "db_insert_failed" ERROR lines show it, and each poll's rows
--   are lost for good. README.md ("Rolling out the rename") lists what each mistake
--   looks like.
--
-- WHAT IT DOES
--   1. ALTER TABLE laddms.tm_events SET SCHEMA geo_feeds   (creates geo_feeds if missing)
--   2. ALTER TABLE geo_feeds.tm_events RENAME TO ticketmaster_events
--   3. renames its indexes tm_events_* -> ticketmaster_events_*  (the primary key
--      constraint follows its index)
--   4. moves and renames the view laddms.v_tm_events_latest -> geo_feeds.v_ticketmaster_events_latest
--      (a move, not a drop and recreate, so its owner and any GRANTs on it survive)
--   5. creates the view from scratch if there was no old one to move
--
-- WHAT IT DELIBERATELY DOES NOT DO
--   * Touch laddms itself, or any other object in it.
--   * Merge two tables. If BOTH laddms.tm_events and geo_feeds.ticketmaster_events exist
--     (e.g. sql/ticketmaster_events.sql was run first), step 1 refuses and says so; decide
--     by hand whether the new one is empty (drop it, then re-run this) or has rows to merge.
--   * Grant anything. Table privileges move with the table, but the service role also needs
--     USAGE on the NEW schema: GRANT USAGE ON SCHEMA geo_feeds TO <app_role>; (see the
--     commented block at the end of sql/ticketmaster_events.sql)
--   * Change the DB_TABLE env var. The service writes whatever DB_TABLE names (default
--     geo_feeds.ticketmaster_events); a deployment that sets DB_TABLE=laddms.tm_events
--     explicitly must change it to the new name, or the service will fail every insert.

-- ---- 1. move the table into geo_feeds ------------------------------------------------
DO $$
BEGIN
    IF to_regclass('laddms.tm_events') IS NULL THEN
        RAISE NOTICE 'step 1 skipped: laddms.tm_events does not exist (already moved, or never created)';
    ELSIF to_regclass('geo_feeds.tm_events') IS NOT NULL
       OR to_regclass('geo_feeds.ticketmaster_events') IS NOT NULL THEN
        RAISE NOTICE 'step 1 REFUSED: laddms.tm_events exists but geo_feeds.tm_events or geo_feeds.ticketmaster_events does too; resolve by hand (see header)';
    ELSE
        CREATE SCHEMA IF NOT EXISTS geo_feeds;
        ALTER TABLE laddms.tm_events SET SCHEMA geo_feeds;
        RAISE NOTICE 'step 1 done: laddms.tm_events -> geo_feeds.tm_events';
    END IF;
END
$$;

-- ---- 2. rename the table --------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('geo_feeds.tm_events') IS NULL THEN
        RAISE NOTICE 'step 2 skipped: geo_feeds.tm_events does not exist (already renamed, or step 1 did not run)';
    ELSIF to_regclass('geo_feeds.ticketmaster_events') IS NOT NULL THEN
        RAISE NOTICE 'step 2 REFUSED: geo_feeds.ticketmaster_events already exists next to geo_feeds.tm_events; resolve by hand';
    ELSE
        ALTER TABLE geo_feeds.tm_events RENAME TO ticketmaster_events;
        RAISE NOTICE 'step 2 done: geo_feeds.tm_events -> geo_feeds.ticketmaster_events';
    END IF;
END
$$;

-- ---- 3. rename the indexes ------------------------------------------------------------
-- tm_events_pkey backs the PRIMARY KEY (id, write_time); renaming an index that backs a
-- constraint renames the constraint too. tm_events_write_time_idx is the time index
-- create_hypertable() made. Each one is renamed only if the old name exists and the new
-- one is free.
DO $$
DECLARE
    suffix text;
BEGIN
    IF to_regclass('geo_feeds.ticketmaster_events') IS NULL THEN
        RAISE NOTICE 'step 3 skipped: geo_feeds.ticketmaster_events does not exist';
        RETURN;
    END IF;
    FOREACH suffix IN ARRAY ARRAY['pkey', 'start_idx', 'city_idx', 'geo_idx',
                                  'last_seen_idx', 'write_time_idx']
    LOOP
        IF to_regclass('geo_feeds.tm_events_' || suffix) IS NULL THEN
            RAISE NOTICE 'step 3 skipped for %: geo_feeds.tm_events_% does not exist', suffix, suffix;
        ELSIF to_regclass('geo_feeds.ticketmaster_events_' || suffix) IS NOT NULL THEN
            RAISE NOTICE 'step 3 REFUSED for %: geo_feeds.ticketmaster_events_% already exists', suffix, suffix;
        ELSE
            EXECUTE format('ALTER INDEX geo_feeds.%I RENAME TO %I',
                           'tm_events_' || suffix, 'ticketmaster_events_' || suffix);
            RAISE NOTICE 'step 3 done: tm_events_% -> ticketmaster_events_%', suffix, suffix;
        END IF;
    END LOOP;
END
$$;

-- ---- 4. move the view ----------------------------------------------------------------
-- Views reference their table by OID, so the old view keeps working through the table's
-- move and rename; it only needs to follow it to the new schema and name. Moving it
-- (rather than dropping and recreating) preserves its owner and grants.
DO $$
BEGIN
    IF to_regclass('laddms.v_tm_events_latest') IS NULL THEN
        RAISE NOTICE 'step 4 skipped: laddms.v_tm_events_latest does not exist (already moved, or never created)';
    ELSIF to_regclass('geo_feeds.v_tm_events_latest') IS NOT NULL
       OR to_regclass('geo_feeds.v_ticketmaster_events_latest') IS NOT NULL THEN
        RAISE NOTICE 'step 4 REFUSED: laddms.v_tm_events_latest exists but geo_feeds.v_tm_events_latest or geo_feeds.v_ticketmaster_events_latest does too; resolve by hand';
    ELSE
        ALTER VIEW laddms.v_tm_events_latest SET SCHEMA geo_feeds;
        ALTER VIEW geo_feeds.v_tm_events_latest RENAME TO v_ticketmaster_events_latest;
        RAISE NOTICE 'step 4 done: laddms.v_tm_events_latest -> geo_feeds.v_ticketmaster_events_latest';
    END IF;
END
$$;

-- ---- 5. create the view if there was none to move --------------------------------------
-- Same definition as in sql/ticketmaster_events.sql. Only once the renamed table is in place.
DO $$
BEGIN
    IF to_regclass('geo_feeds.ticketmaster_events') IS NULL THEN
        RAISE NOTICE 'step 5 skipped: geo_feeds.ticketmaster_events does not exist';
    ELSIF to_regclass('geo_feeds.v_ticketmaster_events_latest') IS NOT NULL THEN
        RAISE NOTICE 'step 5 skipped: geo_feeds.v_ticketmaster_events_latest already exists';
    ELSE
        CREATE VIEW geo_feeds.v_ticketmaster_events_latest AS
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
        RAISE NOTICE 'step 5 done: created geo_feeds.v_ticketmaster_events_latest';
    END IF;
END
$$;
