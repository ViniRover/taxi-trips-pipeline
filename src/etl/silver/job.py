from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from etl.common import PostgresConfig, create_spark, jdbc_read, jdbc_write


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Limpa e deduplica uma tabela bronze na camada silver.")
    parser.add_argument("--table", required=True)
    parser.add_argument("--deduplicate-by", nargs="*", default=[])
    return parser.parse_args()


def clean_strings(df: DataFrame) -> DataFrame:
    expressions = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned = F.trim(F.col(field.name))
            expressions.append(F.when(cleaned == "", None).otherwise(cleaned).alias(field.name))
        else:
            expressions.append(F.col(field.name))
    return df.select(*expressions)


def existing_dedup_keys(df: DataFrame, candidates: list[str]) -> list[str]:
    missing = [key for key in candidates if key not in df.columns]
    if missing:
        raise ValueError(f"Deduplication columns not found in bronze table: {missing}")
    return candidates

def current_month_bronze_trips_query(table_name: str):
    query = f"""
        SELECT * FROM {table_name}
        WHERE ingestion_timestamp >= DATE_TRUNC('month', NOW())
        AND ingestion_timestamp < (
            DATE_TRUNC('month', NOW()) + INTERVAL '1 month'
        )
    """
   
    return query

def main() -> None:
    args = parse_args()
    spark = create_spark(f"silver_{args.table}")
    pg = PostgresConfig()

    try:
        query = current_month_bronze_trips_query(f"bronze.{args.table}" )
        df = jdbc_read(spark=spark, pg=pg, query=query)
        
        jdbc_write(df, pg, f"silver.{args.table}", mode="overwrite", truncate=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
