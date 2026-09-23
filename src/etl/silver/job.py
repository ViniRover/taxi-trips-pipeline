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
    "trip_distance": "decimal",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "vendor_id": "integer",
    "passenger_count": "integer",
    "pu_location_id": "integer",
    "do_location_id": "integer",
    "ratecode_id": "integer",
    "payment_type": "integer",
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Limpa e deduplica uma tabela bronze na camada silver.")
    parser.add_argument("--table", required=True)
    parser.add_argument("--deduplicate-by", nargs="*", default=[])
    parser.add_argument("--fetch-size", type=int, default=1000, help="Rows fetched per JDBC round trip.")
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
        AND tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND tpep_pickup_datetime < tpep_dropoff_datetime
    """
   
    return query

def main() -> None:
    args = parse_args()
    spark = create_spark(f"silver_{args.table}")
    pg = PostgresConfig()

    try:
        query = current_month_bronze_trips_query(f"bronze.{args.table}")
        df = jdbc_read(spark=spark, pg=pg, query=query, fetch_size=args.fetch_size)

        for key, data_type in parse_key_type.items():
            if data_type == "decimal":
               df = df.withColumn(key, df[key].cast(DecimalType(10,2)))
            else:
               df = df.withColumn(key, df[key].cast(data_type))

        df = df.drop("request_source")
        df = (
            df
            .withColumn(
                "_duration_seconds",
                F.timestamp_diff(
                    "SECOND",
                    "tpep_pickup_datetime",
                    "tpep_dropoff_datetime",
                ),
            )
            .withColumn(
                "has_valid_duration",
                F.col("_duration_seconds").between(60, 180 * 60)
                & (F.col("trip_distance") >= 1)
            )
            .withColumn(
                "trip_duration_minutes",
                (F.col("_duration_seconds") / F.lit(60.0))
                .cast(DecimalType(10, 2)),
            )
            .withColumn(
                "has_valid_fare",
                F.col("total_amount") > 0
            )
            .withColumn(
                "trip_speed_mph",
                F.when(
                    F.col("has_valid_duration"),
                    F.round(
                        F.col("trip_distance") * F.lit(3600.0)
                        / F.col("_duration_seconds"),
                        2,
                    ),
                ),
            )
            .withColumn(
                "has_valid_speed",
                F.col("trip_speed_mph").isNotNull()
                & (F.col("trip_speed_mph") <= 70),
            )
            .withColumn(
                "trip_speed_mph",
                F.when(
                    F.col("has_valid_speed"),
                    F.col("trip_speed_mph"),
                ),
            )
            .drop(
                "_duration_seconds",
            )
        )
        df = df.withColumnRenamed("ingestion_timestamp", "bronze_ingestion_timestamp").drop("source_row_hash")
        df = clean_strings(df)
        
        jdbc_write(df, pg, f"silver.{args.table}", mode="overwrite", truncate=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
