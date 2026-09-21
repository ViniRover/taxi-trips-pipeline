\set ON_ERROR_STOP on
BEGIN;
LOCK TABLE bronze.yellow_taxi_trips IN ACCESS EXCLUSIVE MODE;

ALTER TABLE bronze.yellow_taxi_trips ADD COLUMN IF NOT EXISTS source_row_hash text;

CREATE OR REPLACE FUNCTION bronze.taxi_row_hash(payload jsonb)
RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT encode(sha256(convert_to(
        (payload - ARRAY['ingestion_timestamp', 'source_row_hash'])::text,
        'UTF8'
    )), 'hex');
$$;

CREATE OR REPLACE FUNCTION bronze.set_taxi_row_hash()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.source_row_hash := bronze.taxi_row_hash(to_jsonb(NEW));
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS set_taxi_row_hash ON bronze.yellow_taxi_trips;
CREATE TRIGGER set_taxi_row_hash
BEFORE INSERT OR UPDATE ON bronze.yellow_taxi_trips
FOR EACH ROW EXECUTE FUNCTION bronze.set_taxi_row_hash();

CREATE TABLE IF NOT EXISTS bronze.taxi_duplicate_archive (
    archived_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    original_row jsonb NOT NULL
);

DO $$
BEGIN
    IF to_regclass('bronze.uq_yellow_taxi_file_row_hash') IS NULL THEN
        DROP INDEX IF EXISTS bronze.uq_yellow_taxi_source_row_hash;
        UPDATE bronze.yellow_taxi_trips AS y
        SET source_row_hash = bronze.taxi_row_hash(to_jsonb(y));

        WITH ranked AS (
            SELECT ctid AS row_tid,
                   row_number() OVER (
                       PARTITION BY source_row_hash
                       ORDER BY ingestion_timestamp NULLS LAST, ctid
                   ) AS occurrence
            FROM bronze.yellow_taxi_trips
        ), duplicates AS (
            SELECT y.ctid AS row_tid, to_jsonb(y) AS original_row
            FROM bronze.yellow_taxi_trips AS y
            JOIN ranked AS r ON y.ctid = r.row_tid
            WHERE r.occurrence > 1
        ), archived AS (
            INSERT INTO bronze.taxi_duplicate_archive (original_row)
            SELECT original_row FROM duplicates
            RETURNING original_row
        )
        DELETE FROM bronze.yellow_taxi_trips AS y
        USING duplicates AS d
        WHERE y.ctid = d.row_tid;

        CREATE UNIQUE INDEX uq_yellow_taxi_file_row_hash
            ON bronze.yellow_taxi_trips(source_row_hash);
    END IF;
END;
$$;

ALTER TABLE bronze.yellow_taxi_trips ALTER COLUMN source_row_hash SET NOT NULL;
COMMIT;

BEGIN;

LOCK TABLE bronze.yellow_taxi_trips
IN ACCESS EXCLUSIVE MODE;

ALTER TABLE bronze.yellow_taxi_trips
ADD COLUMN IF NOT EXISTS request_source text;

UPDATE bronze.yellow_taxi_trips AS y
SET source_row_hash = bronze.taxi_row_hash(to_jsonb(y));

COMMIT;
