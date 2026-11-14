FROM apache/airflow:2.10.5-python3.11

# Install the providers against Airflow's published constraints so nothing upgrades Airflow itself.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow[amazon,databricks]==2.10.5" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
