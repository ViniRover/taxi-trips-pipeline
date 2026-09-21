"""Optional integration tests against a disposable, migrated PostgreSQL database."""

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from etl.bronze.ingestion import RowIngestion


@pytest.fixture
def postgres_target():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set TEST_POSTGRES_DSN for a disposable migrated database")
    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    parameters = connection.get_dsn_parameters()
    pg = SimpleNamespace(
        host=parameters.get("host", "localhost"), port=parameters.get("port", "5432"),
        database=parameters["dbname"], user=parameters["user"],
        password=connection.info.password,
    )
    table = f"row_test_{uuid4().hex}"
    with connection.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE bronze.{table} (
                ingestion_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                source_file text NOT NULL,
                trip_distance text,
                source_row_hash text NOT NULL UNIQUE
            );
            CREATE TRIGGER set_hash BEFORE INSERT OR UPDATE ON bronze.{table}
            FOR EACH ROW EXECUTE FUNCTION bronze.set_taxi_row_hash();
        """)
    try:
        yield pg, table, connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS bronze.{table}")
        connection.close()


def load(pg, table, rows):
    with RowIngestion(pg, table) as ingestion:
        ingestion.create_stage()
        with ingestion.connection.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {ingestion.stage_table} (source_file, trip_distance) VALUES (%s, %s)",
                rows,
            )
        return ingestion.publish()


def test_duplicates_reruns_and_different_files(postgres_target):
    pg, table, connection = postgres_target
    assert load(pg, table, [("first", "1.0"), ("first", "1.0"), ("first", None)]) == 2
    assert load(pg, table, [("second", "1.0"), ("second", None), ("second", "")]) == 3
    assert load(pg, table, [("first", "1.0")]) == 0
    assert load(pg, table, [("first", "2.0")]) == 1
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT count(*) FROM bronze.{table}")
        assert cursor.fetchone()[0] == 6


def test_failed_merge_preserves_published_rows(postgres_target):
    pg, table, connection = postgres_target
    assert load(pg, table, [("first", "1.0")]) == 1
    with RowIngestion(pg, table) as ingestion:
        ingestion.create_stage()
        with ingestion.connection.cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO {ingestion.stage_table} (source_file, trip_distance)
                VALUES ('second', '2.0'), ('second', 'bad')
            """)
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER TABLE bronze.{table} ADD CHECK (trip_distance <> 'bad')")
        with pytest.raises(Exception):
            ingestion.publish()
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT trip_distance FROM bronze.{table}")
        assert cursor.fetchall() == [("1.0",)]
