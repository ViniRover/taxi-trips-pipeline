FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow==2.10.5 \
    apache-airflow-providers-apache-spark==5.1.1 \
    pyspark==3.5.5 \
    && airflow version \
    && spark-submit --version

USER root
RUN mkdir -p /opt/jdbc \
    && curl --fail --location --retry 3 \
      https://jdbc.postgresql.org/download/postgresql-42.7.5.jar \
      --output /opt/jdbc/postgresql.jar \
    && chown -R airflow:root /opt/jdbc

USER airflow
