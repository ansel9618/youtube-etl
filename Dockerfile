# We use python 3.9 due to some packages from requirements.txt requiring version >= 3.8
# Run the below command to create an extended image using Dockerfile:
# docker build . -f Dockerfile --pull --tag extending-image
# Then we use this image (extending-image:0.0.1) in the docker compose file
# AIRFLOW_HOME=/opt/airflow is the default

ARG  AIRFLOW_VERSION=2.5.3

FROM apache/airflow:${AIRFLOW_VERSION}-python3.9

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt