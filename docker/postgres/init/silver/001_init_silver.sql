CREATE TABLE IF NOT EXISTS silver.yellow_taxi_trips (
    trip_id BIGSERIAL PRIMARY KEY,

    bronze_ingestion_timestamp TIMESTAMP,
    silver_ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255) NOT NULL,

    vendor_id INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(10,2),
    
    pu_location_id VARCHAR(50),
    do_location_id VARCHAR(50),

    ratecode_id INTEGER,
    store_and_fwd_flag CHAR(1),
    payment_type INTEGER,

    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    cbd_congestion_fee DECIMAL(10,2),

    has_valid_fare BOOLEAN,
    has_valid_duration BOOLEAN,
    has_valid_speed BOOLEAN,

    trip_duration_minutes DECIMAL(10,2),
    trip_speed_mph DECIMAL(5,2)
);

CREATE INDEX IF NOT EXISTS idx_silver_pickup_datetime ON silver.yellow_taxi_trips(tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_silver_dropoff_datetime ON silver.yellow_taxi_trips(tpep_dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_silver_vendor_id ON silver.yellow_taxi_trips(vendor_id);
CREATE INDEX IF NOT EXISTS idx_silver_payment_type ON silver.yellow_taxi_trips(payment_type);
CREATE INDEX IF NOT EXISTS idx_silver_valid_trips ON silver.yellow_taxi_trips(has_valid_fare, has_valid_duration, has_valid_speed);
