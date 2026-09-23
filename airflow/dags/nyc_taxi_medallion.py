from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

CONFIG_PATH = "/opt/pipeline/config/pipeline.yml"
JDBC_JAR = "/opt/jdbc/postgresql.jar"
PYTHON_PATH = "/opt/pipeline/src"

with open(CONFIG_PATH, encoding="utf-8") as config_file:
    CONFIG = yaml.safe_load(config_file)["pipeline"]

SOURCE = CONFIG["source"]
TABLE = CONFIG["table"]
DOWNLOADED_FILE = "{{ ti.xcom_pull(task_ids='download_yellow_taxi_parquet') }}"


def is_parquet(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size <= 8:
        return False
    with path.open("rb") as parquet_file:
        header = parquet_file.read(4)
        parquet_file.seek(-4, 2)
        footer = parquet_file.read(4)
    return header == footer == b"PAR1"


def year_month_before(year: int, month: int, months_before: int) -> tuple[int, int]:
    month_index = year * 12 + month - 1 - months_before
    return divmod(month_index, 12)[0], divmod(month_index, 12)[1] + 1


def download_parquet(base_url: str, taxi_type: str, input_dir: str, max_lookback_months: int) -> str:
    """Download the newest published TLC month, starting with the current month."""
    current = datetime.now(ZoneInfo("America/New_York"))
    destination_dir = Path(input_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)

    for months_before in range(max_lookback_months + 1):
        year, month = year_month_before(current.year, current.month, months_before)
        file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        target = destination_dir / file_name
        if is_parquet(target):
            return str(target)

        url = f"{base_url}/{file_name}"
        partial = target.with_suffix(target.suffix + ".part")
        try:
            with urlopen(url, timeout=60) as response, partial.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
            if not is_parquet(partial):
                raise ValueError(f"Downloaded file is not valid Parquet: {url}")
            partial.replace(target)
            return str(target)
        except HTTPError as error:
            if error.code not in  (404, 403):
                raise
        finally:
            partial.unlink(missing_ok=True)

    raise FileNotFoundError(
        f"No published {taxi_type} taxi Parquet file found in the last "
        f"{max_lookback_months + 1} months"
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_medallion",
    description="NYC TLC Parquet",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi", "pyspark", "medallion"],
) as dag:
    start = EmptyOperator(task_id="start")
    download = PythonOperator(
        task_id="download_yellow_taxi_parquet",
        python_callable=download_parquet,
        op_kwargs={
            "base_url": SOURCE["base_url"],
            "taxi_type": SOURCE["taxi_type"],
            "input_dir": CONFIG["input_dir"],
            "max_lookback_months": SOURCE["max_lookback_months"],
        },
    )
    spark_conf = {
        "spark.executor.instances": "1",
        "spark.executor.cores": "1",
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
    }
    bronze = SparkSubmitOperator(
        task_id="bronze_yellow_taxi_trips",
        application="/opt/pipeline/src/etl/bronze/job.py",
        conn_id="spark_default",
        jars=JDBC_JAR,
        application_args=["--table", TABLE["name"], "--file", DOWNLOADED_FILE],
        conf=spark_conf,
        env_vars={"PYTHONPATH": PYTHON_PATH},
        verbose=False,
    )
    silver = SparkSubmitOperator(
        task_id="silver_yellow_taxi_trips",
        application="/opt/pipeline/src/etl/silver/job.py",
        conn_id="spark_default",
        jars=JDBC_JAR,
        application_args=[
            "--table",
            TABLE["name"],
            "--fetch-size",
            str(CONFIG["silver"]["jdbc_fetch_size"]),
        ],
        conf=spark_conf,
        env_vars={"PYTHONPATH": PYTHON_PATH},
        verbose=False,
    )
    complete = EmptyOperator(task_id="silver_complete")

    start >> download >> bronze >> silver >> complete
