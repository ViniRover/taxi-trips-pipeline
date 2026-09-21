from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import DecimalType

from etl.common import PostgresConfig, create_spark, jdbc_read, jdbc_write

parse_key_type = {
    "fare_amount": "decimal",
    "extra": "decimal",
    "mta_tax": "decimal",
    "tip_amount": "decimal",
    "tolls_amount": "decimal",
    "improvement_surcharge": "decimal",
    "total_amount": "decimal",
    "congestion_surcharge": "decimal",
    "airport_fee": "decimal",
    "cbd_congestion_fee": "decimal",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "vendor_id": "integer",
    "passenger_count": "integer",
    "trip_distance": "integer",
    "pu_location_id": "integer",
    "do_location_id": "integer",
    "ratecode_id": "integer",
    "payment_type": "integer",
}

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
        query = current_month_bronze_trips_query(f"bronze.{args.table}")
        df = jdbc_read(spark=spark, pg=pg, query=query)

        for key, data_type in parse_key_type.items():
            if data_type == "decimal":
               df_matches = df_matches.withColumn(key, df_matches[key].cast(DecimalType(10,2)))
            else:
               df_matches = df_matches.withColumn(key, df_matches[key].cast(data_type))

        df_matches = df_matches.drop("request_source")
        df_matches = df_matches.withColumn("trip_duration_minutes", F.timestamp_diff("SECOND", "tpep_pickup_datetime", "tpep_dropoff_datetime") / 60)
        df_matches = df_matches.withColumn("trip_duration_minutes", df_matches["trip_duration_minutes"].cast(DecimalType(10,2)))
        
        jdbc_write(df, pg, f"silver.{args.table}", mode="overwrite", truncate=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
