from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl.common import PostgresConfig, create_spark, jdbc_write
from etl.bronze.ingestion import RowIngestion

COLUMN_NAMES = {
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load one NYC TLC Parquet file into bronze.")
    parser.add_argument("--table", default="yellow_taxi_trips")
    parser.add_argument("--file", required=True)
    return parser.parse_args()


def canonicalize(source: DataFrame, source_file: str) -> DataFrame:
    for original, normalized in COLUMN_NAMES.items():
        if original in source.columns:
            source = source.withColumnRenamed(original, normalized)

    return (
        source.withColumn("source_file", F.lit(source_file))
    )


def main() -> None:
    args = parse_args()
    input_file = Path(args.file)
    if not input_file.is_file():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    pg = PostgresConfig()
    with RowIngestion(pg, args.table) as ingestion:
        columns = ingestion.create_stage()
        spark = create_spark(f"bronze_{args.table}")
        try:
            source = canonicalize(spark.read.parquet(str(input_file)), input_file.name)
            unknown = set(source.columns) - set(columns) - {"source_file"}
            if unknown:
                raise ValueError(f"Source columns missing from bronze DDL: {sorted(unknown)}")
            
            bronze = source.select(
                F.col("source_file"),
                *[
                    (F.col(name) if name in source.columns else F.lit(None)).cast("string").alias(name)
                    for name in columns
                ],
            )
            jdbc_write(bronze, pg, ingestion.stage_table, mode="append")
            row_count = ingestion.publish()
            print(f"Completed {input_file.name}: {row_count} new rows inserted; existing rows skipped")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
