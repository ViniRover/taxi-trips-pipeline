from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl.common import PostgresConfig, create_spark, jdbc_write

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

    spark = create_spark(f"bronze_{args.table}")
    try:
        source = spark.read.parquet(str(input_file))
        bronze = canonicalize(source, input_file.name)
        jdbc_write(bronze, PostgresConfig(), f"bronze.{args.table}", mode="append")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
