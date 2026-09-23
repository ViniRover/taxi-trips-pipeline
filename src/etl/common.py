from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "host")
    port: str = os.getenv("POSTGRES_PORT", "5432")
    database: str = os.getenv("POSTGRES_DB", "database")
    user: str = os.getenv("POSTGRES_USER", "user")
    password: str = os.getenv("POSTGRES_PASSWORD", "password")

    @property
    def url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def options(self) -> dict[str, str]:
        return {
            "url": self.url,
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def jdbc_read(
    spark: SparkSession,
    pg: PostgresConfig,
    table: str = None,
    query: str = None,
    fetch_size: int = 1000,
) -> DataFrame:
    if fetch_size <= 0:
        raise ValueError("JDBC fetch size must be greater than zero")

    reader = spark.read.format("jdbc").options(**pg.options).option("fetchsize", str(fetch_size))
    if query:
        return reader.option("query", query).load()

    return reader.option("dbtable", table).load()

def jdbc_write(df: DataFrame, pg: PostgresConfig, table: str, mode: str, truncate: bool = False) -> None:
    writer = df.write.format("jdbc").options(**pg.options).option("dbtable", table).mode(mode)
    if truncate:
        writer = writer.option("truncate", "true")
    writer.save()
