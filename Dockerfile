FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pyspark pika pydantic-settings psycopg2-binary
