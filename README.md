# Row-level idempotent bronze ingestion

Every input is loaded into a uniquely named staging table. The final PostgreSQL
transaction inserts rows into bronze with `ON CONFLICT (source_row_hash) DO
NOTHING`. Existing rows are never replaced or deleted by normal ingestion.
Concurrent merges are serialized by a transaction advisory lock.

PostgreSQL computes a SHA-256 hash from a JSONB representation of all stored
trip-data columns plus `source_file`. Ingestion timestamp and the hash itself are
excluded. Null and empty values remain distinct. Identical trip data is skipped
within the same file, on executor retries, and on job reruns. Identical trips
in differently named files are stored separately.
Changed trip values count as new rows, not updates to an existing trip.

NYC Taxi has no guaranteed unique trip ID: separate real trips with identical
field values within the same file are deliberately treated as duplicates. Deduplication uses the
stored text values, not fuzzy matching or numeric normalization. The first
inserted copy retains its original filename and ingestion timestamp.

## Apply changes

Pause ingestion while applying the migration, then run:

```powershell
docker compose stop airflow-scheduler
docker compose run --rm postgres-bootstrap
docker compose up --build -d
```

The migration in `docker/postgres/init/bronze/002_row_deduplication.sql` locks
bronze, hashes existing records, archives duplicate copies in
`bronze.taxi_duplicate_archive`, keeps the earliest ingested copy, and adds a
unique index. This is transactional and repeatable, but may take time and disk
space on a large table. Archived records include their original metadata and
are recoverable; reinserting exact duplicates requires explicitly changing the
deduplication policy first.

Upgrading from the filename-independent policy automatically recalculates
existing hashes and replaces the old index. Reload original files to recover
cross-file rows previously removed by that policy; archive records are not
automatically restored.

The old `bronze.ingestion_files` table is no longer consulted or created. An
existing copy is left untouched for historical reference.

## Validation and operations

Run `pytest tests` and `ruff check src/etl/bronze tests`.
Two optional PostgreSQL integration tests require `TEST_POSTGRES_DSN` pointing
to a disposable database where the migration has already been applied.

No file is skipped based on its name or checksum. A forcible process termination
may leave an unused `bronze.ingest_*` staging table without changing published
bronze data. Remove orphan tables only after confirming no ingestion is running.
Future trip-schema changes require a corresponding hash/index migration.
