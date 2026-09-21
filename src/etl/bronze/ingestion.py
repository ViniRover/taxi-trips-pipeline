from __future__ import annotations

import re
from uuid import uuid4


def validate_table(table: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
        raise ValueError(f"Invalid bronze table name: {table}")
    return table


class RowIngestion:
    """Stage rows and let PostgreSQL insert only previously unseen trip hashes."""

    def __init__(self, pg, table: str):
        import psycopg2
        from psycopg2 import sql

        self.sql = sql
        self.table = validate_table(table)
        self.stage_name = f"ingest_{uuid4().hex}"
        self.stage_table = f"bronze.{self.stage_name}"
        self.connection = psycopg2.connect(
            host=pg.host, port=pg.port, dbname=pg.database,
            user=pg.user, password=pg.password,
        )
        self.connection.autocommit = True

    def __enter__(self):
        return self

    def create_stage(self) -> list[str]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'bronze' AND table_name = %s ORDER BY ordinal_position",
                (self.table,),
            )
            self.columns = [row[0] for row in cursor.fetchall() if row[0] != "source_row_hash"]
            cursor.execute(self.sql.SQL(
                "CREATE TABLE bronze.{} (LIKE bronze.{} INCLUDING DEFAULTS)"
            ).format(self.sql.Identifier(self.stage_name), self.sql.Identifier(self.table)))
            cursor.execute(self.sql.SQL(
                "ALTER TABLE bronze.{} ALTER COLUMN source_row_hash DROP NOT NULL"
            ).format(self.sql.Identifier(self.stage_name)))
        return [name for name in self.columns if name not in ("ingestion_timestamp", "source_file")]

    def publish(self) -> int:
        self.connection.autocommit = False
        try:
            with self.connection:
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"bronze.{self.table}:row_merge",),
                    )
                    columns = self.sql.SQL(", ").join(self.sql.Identifier(name) for name in self.columns)
                    cursor.execute(self.sql.SQL(
                        "INSERT INTO bronze.{} ({}) SELECT {} FROM bronze.{} "
                        "ON CONFLICT (source_row_hash) DO NOTHING"
                    ).format(
                        self.sql.Identifier(self.table), columns, columns,
                        self.sql.Identifier(self.stage_name),
                    ))
                    inserted = cursor.rowcount
            return inserted
        finally:
            self.connection.autocommit = True

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql.SQL("DROP TABLE IF EXISTS bronze.{}").format(
                    self.sql.Identifier(self.stage_name)
                ))
        finally:
            self.connection.close()
