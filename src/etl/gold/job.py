from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window

from etl.common import PostgresConfig, create_spark, jdbc_read, jdbc_write

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Limpa e deduplica uma tabela bronze na camada silver.")
    parser.add_argument("--table", required=True)
    parser.add_argument("--fetch-size", type=int, default=1000, help="Rows fetched per JDBC round trip.")
    return parser.parse_args()

def create_daily_trip_summary(table_name: str) -> str:
   daily_trip_summary_query = f"""
      SELECT
         DATE(tpep_pickup_datetime) as trip_date,
         COUNT(*) as total_trips,
         COUNT(DISTINCT vendor_id) as active_vendors,
         
         SUM(passenger_count) as total_passengers,
         ROUND(AVG(passenger_count), 2) as avg_passengers_per_trip,
         
         ROUND(SUM(trip_distance), 2) as total_distance_miles,
         ROUND(AVG(trip_distance), 2) as avg_trip_distance,
         ROUND(AVG(trip_duration_minutes), 2) as avg_trip_duration_minutes,
         ROUND(AVG(trip_speed_mph), 2) as avg_speed_mph,
         
         ROUND(SUM(fare_amount), 2) as total_fare_amount,
         ROUND(SUM(tip_amount), 2) as total_tips,
         ROUND(SUM(total_amount), 2) as total_revenue,
         ROUND(AVG(total_amount), 2) as avg_trip_cost,
         ROUND(MAX(total_amount), 2) as max_trip_cost,
         
         ROUND(100.0 * SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as tip_rate_percentage,
         ROUND(AVG(CASE WHEN tip_amount > 0 THEN tip_amount ELSE NULL END), 2) as avg_tip_when_given,
         
         SUM(CASE WHEN has_valid_fare THEN 1 ELSE 0 END) as trips_with_valid_fare,
         SUM(CASE WHEN has_valid_fare AND has_valid_duration THEN 1 ELSE 0 END) as fully_valid_trips
      FROM {table_name}
      GROUP BY DATE(tpep_pickup_datetime)
   """

   return daily_trip_summary_query;

def create_vendor_performance(table_name: str) -> str:
   vendor_performance_query = f"""
      SELECT 
         vendor_id,
         CASE
            WHEN vendor_id = 1 THEN 'Creative Mobile Technologies'
            WHEN vendor_id = 2 THEN 'Verifone Inc'
            ELSE 'Other'
         END as vendor_name,
         
         COUNT(*) as total_trips,
         COUNT(DISTINCT DATE(tpep_pickup_datetime)) as days_active,
         ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(tpep_pickup_datetime)), 2) as avg_trips_per_day,
         
         ROUND(AVG(trip_distance), 2) as avg_trip_distance,
         ROUND(AVG(trip_duration_minutes), 2) as avg_trip_duration,
         ROUND(AVG(trip_speed_mph), 2) as avg_speed_mph,
         ROUND(AVG(passenger_count), 2) as avg_passengers,
         
         ROUND(SUM(total_amount), 2) as total_revenue,
         ROUND(AVG(total_amount), 2) as avg_revenue_per_trip,
         ROUND(SUM(total_amount) / COUNT(DISTINCT DATE(tpep_pickup_datetime)), 2) as avg_daily_revenue,
         
         ROUND(SUM(tip_amount), 2) as total_tips,
         ROUND(AVG(CASE WHEN tip_amount > 0 THEN tip_amount ELSE NULL END), 2) as avg_tip_when_given,
         ROUND(100.0 * SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as tip_rate_percentage,
         
         ROUND(100.0 * SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as credit_card_percentage,
         ROUND(100.0 * SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) / COUNT(*), 2) as cash_percentage,

         ROUND(100.0 * SUM(CASE WHEN has_valid_fare AND has_valid_duration THEN 1 ELSE 0 END) / COUNT(*), 2) as data_quality_score
      from {table_name}
      group by vendor_id
   """

   return vendor_performance_query

def create_payment_type_analysis(silver_df: DataFrame) -> DataFrame:
   grouped = (
      silver_df
      .filter(F.col("has_valid_fare"))
      .groupBy("payment_type")
      .agg(
         F.count("*").alias("trip_count"),
         F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
         F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration"),
         F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
         F.sum("total_amount").alias("_revenue_sum"),
         F.round(F.avg("total_amount"), 2).alias("avg_trip_amount"),
         F.round(F.min("total_amount"), 2).alias("min_trip_amount"),
         F.round(F.max("total_amount"), 2).alias("max_trip_amount"),
         F.round(F.sum("tip_amount"), 2).alias("total_tips"),
         F.round(
               F.avg(F.when(F.col("tip_amount") > 0, F.col("tip_amount"))),
               2,
         ).alias("avg_tip_when_given"),
         F.sum(
               F.when(F.col("tip_amount") > 0, 1).otherwise(0)
         ).alias("_tipped_trips"),
      )
   )

   all_payment_types = Window.partitionBy()

   payment_summary = (
      grouped
      .withColumn(
         "payment_method",
         F.when(F.col("payment_type") == 1, "Credit Card")
            .when(F.col("payment_type") == 2, "Cash")
            .when(F.col("payment_type") == 3, "No Charge")
            .when(F.col("payment_type") == 4, "Dispute")
            .otherwise("Other"),
      )
      .withColumn(
         "percentage_of_trips",
         F.round(
               100.0 * F.col("trip_count")
               / F.sum("trip_count").over(all_payment_types),
               2,
         ),
      )
      .withColumn(
         "percentage_of_revenue",
         F.round(
               100.0 * F.col("_revenue_sum")
               / F.sum("_revenue_sum").over(all_payment_types),
               2,
         ),
      )
      .withColumn("total_revenue", F.round(F.col("_revenue_sum"), 2))
      .withColumn(
         "tip_rate_percentage",
         F.round(100.0 * F.col("_tipped_trips") / F.col("trip_count"), 2),
      )
      .drop("_revenue_sum", "_tipped_trips")
   )

   return payment_summary

def main() -> None:
   args = parse_args()
   spark = create_spark(f"silver_{args.table}")
   pg = PostgresConfig()

   silver_table_name = f"silver.{args.table}"
   daily_trip_summary_query = create_daily_trip_summary(silver_table_name)
   vendor_performance_query = create_vendor_performance(silver_table_name)

   daily_summary_trip_df = jdbc_read(spark, pg, query=daily_trip_summary_query, fetch_size=args.fetch_size)
   vendor_performance_df = jdbc_read(spark, pg, query=vendor_performance_query, fetch_size=args.fetch_size)

   jdbc_write(daily_summary_trip_df, pg, "gold.daily_trip_summary", mode="errorifexists")
   jdbc_write(vendor_performance_df, pg, "gold.vendor_performance", mode="errorifexists")

   # Just performing another way to aggregate with Spark and not SQL queries
   silver_df = jdbc_read(spark, pg, table=silver_table_name, fetch_size=args.fetch_size)
   payment_type_analysis_df = create_payment_type_analysis(silver_df)

   jdbc_write(payment_type_analysis_df, pg, "gold.payment_type_analysis", mode="errorifexists")

if __name__ == "__main__":
   main()
