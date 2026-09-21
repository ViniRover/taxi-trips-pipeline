CREATE TABLE IF NOT EXISTS bronze.yellow_taxi_trips (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255) NOT NULL,

    vendor_id VARCHAR(50),
    tpep_pickup_datetime VARCHAR(50),
    tpep_dropoff_datetime VARCHAR(50),
    passenger_count VARCHAR(50),
    trip_distance VARCHAR(50),
    ratecode_id VARCHAR(50),
    store_and_fwd_flag VARCHAR(50),
    pu_location_id VARCHAR(50),
    do_location_id VARCHAR(50),
    payment_type VARCHAR(50),
    fare_amount VARCHAR(50),
    extra VARCHAR(50),
    mta_tax VARCHAR(50),
    tip_amount VARCHAR(50),
    tolls_amount VARCHAR(50),
    improvement_surcharge VARCHAR(50),
    total_amount VARCHAR(50),
    congestion_surcharge VARCHAR(50),
    airport_fee VARCHAR(50),
    cbd_congestion_fee VARCHAR(50),
    request_source VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_timestamp ON bronze.yellow_taxi_trips(ingestion_timestamp);

CREATE INDEX IF NOT EXISTS idx_bronze_source_file ON bronze.yellow_taxi_trips(source_file);

\ir 002_row_deduplication.sql
