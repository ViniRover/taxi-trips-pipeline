# 🚕 NYC Yellow Taxi Medallion ETL Pipeline

A data engineering project that processes NYC TLC Yellow Taxi trip data through a bronze, silver, and gold architecture. Docker Compose runs Apache Airflow, Apache Spark, and PostgreSQL. Airflow orchestrates the pipeline, while PySpark reads and transforms the trip data.

The pipeline downloads a monthly Parquet file from NYC TLC and loads its records into the bronze layer. Bronze preserves the source data and uses a row hash to prevent duplicate records from the same file. The silver layer converts data types and calculates trip duration, speed, and data-quality flags. The gold layer produces analytics tables for daily trips, vendor performance, and payment methods.

The DAG is currently triggered manually. This project demonstrates ingestion, orchestration, row-level deduplication, data cleaning, and analytical aggregation.

</br>

# 🏗️ Architecture

![alt text](https://github.com/ViniRover/taxi-trips-pipeline/blob/master/img/pipeline-diagram.png)

</br>

# 🛠️ Technologies

| Technology | Purpose |
| --- | --- |
| Python | Pipeline development |
| Apache Spark | Data  processing, manipulation and transformatation |
| Apache Airflow | Pipeline orchestration |
| PostgreSQL | Database |
| Docker Compose | Manage multi-container application |

</br>

# 📂 Project Structure

```base
  .
  ├── airflow/dags/
  │   └── nyc_taxi_medallion.py
  │ 
  ├── config/
  │   └── pipeline.yml
  │ 
  ├── data/input/
  │   └── yellow_tripdata_2026-07.parquet
  │ 
  ├── docker/
  │   ├── airflow/
  │   │  └── Dockerfile
  │   │ 
  │   └── postgres/init
  │       ├── bronze/
  │       │   ├── 001_init_bronze.sql
  │       │   └── 002_row_deduplication.sql
  │       ├── silver/
  │       │   └── 001_init_silver.sql
  │       └── 001_init.sql
  │ 
  ├── src/etl/
  │   │    ├── bronze/
  │   │    │   ├── ingestion.py
  │   │    │   └── job.py
  │   │    ├── gold/
  │   │    │   └── job.py
  │   │    └── silver/
  │   │    │   └── job.py
  │   └── common.py
  │ 
  ├── .env.example
  ├── Makefile
  ├── README.md
  ├── docker-compose.yml
  ├── pyproject.toml
  └── requirements-dev.txt
```

> [!NOTE]
> The file structure may change during the development of the project.

</br>

# ▶️ How to run

## 1. Clone repository

```
  git clone https://github.com/ViniRover/taxi-trips-pipeline.git
```

## 2. Enter inside the folder
```
  cd taxi-trips-pipeline
```

## 3. Configure environment variables
Create a file `.env` containing

```ruby
  POSTGRES_USER=
  POSTGRES_PASSWORD=
  POSTGRES_DB=
  POSTGRES_PORT=5433
  AIRFLOW_PORT=8080
  AIRFLOW_ADMIN_USER=admin
  AIRFLOW_ADMIN_PASSWORD=admin
  SPARK_MASTER_PORT=7077
  SPARK_UI_PORT=8081
  SPARK_WORKER_UI_PORT=8082
  BRONZE_CREATE_IF_MISSING=false
```

## 4. Execute containers

```
  docker compose up --build
```

## 5. Access Airflow

```
  http://localhost:8080/login
```

</br>

# 📄 License

This project is licensed under the MIT License.

Feel free to use it for educational purposes.

