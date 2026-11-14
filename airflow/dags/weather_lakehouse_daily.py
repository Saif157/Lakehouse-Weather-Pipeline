from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from weather_pipeline.airflow_compat import dag, task
from weather_pipeline.alerts import notify_failure
from weather_pipeline.config import (
    CITIES,
    OLIST_PREFIX,
    RAW_PREFIX,
    Settings,
    pick_source,
)
from weather_pipeline.open_meteo_client import ApiResponseError, OpenMeteoClient
from weather_pipeline.s3_io import build_raw_key, put_json
from weather_pipeline.weather_datasets import GOLD_DATASET

log = logging.getLogger(__name__)

SETTINGS = Settings.from_env()

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="weather_lakehouse_daily",
    description="Open-Meteo REST API -> S3 raw -> Databricks (PySpark/Delta) -> Gold Parquet",
    schedule="0 3 * * *", 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "rest-api", "databricks", "athena"],
    doc_md=__doc__,
)
def weather_lakehouse_daily():
    @task
    def list_cities() -> list[dict]:
        return [city.as_dict() for city in CITIES]

    @task(pool="open_meteo_api", retries=4, execution_timeout=timedelta(minutes=5))
    def extract_city(city: dict, ds=None, data_interval_end=None) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        target = date.fromisoformat(ds)
        source = pick_source(target, today=data_interval_end.date())
        try:
            envelope = OpenMeteoClient().fetch_hourly(
                city_slug=city["slug"],
                latitude=city["latitude"],
                longitude=city["longitude"],
                start=target,
                end=target,
                source=source,
            )
        except ApiResponseError as exc:
            # Permanent problem (bad request, malformed payload): retrying cannot help.
            raise AirflowFailException(f"{city['slug']}: {exc}") from exc

        fetched_at = datetime.fromisoformat(envelope["meta"]["fetched_at"])
        key = build_raw_key(RAW_PREFIX, target, city["slug"], fetched_at)
        hook = S3Hook(aws_conn_id=SETTINGS.aws_conn_id)
        size = put_json(hook, SETTINGS.data_bucket, key, envelope)
        hours = len(envelope["payload"]["hourly"]["time"])
        log.info("%s: %d hourly rows from the %s endpoint -> s3://%s/%s (%d bytes)",
                 city["slug"], hours, source, SETTINGS.data_bucket, key, size)
        return {"city": city["slug"], "key": key, "hours": hours, "bytes": size, "source": source}

    @task
    def validate_extraction(results: list[dict]) -> dict:
        """Gate: do not start a paid Databricks cluster on incomplete input."""
        results = list(results)
        expected = {city.slug for city in CITIES}
        got = {r["city"] for r in results}
        if got != expected:
            raise AirflowFailException(f"Missing cities: {sorted(expected - got)}")
        short = sorted(r["city"] for r in results if r["hours"] < 24)
        if short:
            raise AirflowFailException(f"Fewer than 24 hourly rows for: {short}")
        summary = {
            "cities": len(results),
            "hours": sum(r["hours"] for r in results),
            "bytes": sum(r["bytes"] for r in results),
            "sources": sorted({r["source"] for r in results}),
        }
        log.info("Extraction validated: %s", summary)
        return summary

    @task
    def check_olist_data() -> str:
        """The optional Olist enrichment only runs if its CSVs have been uploaded."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        present = S3Hook(aws_conn_id=SETTINGS.aws_conn_id).check_for_key(
            f"{OLIST_PREFIX}/olist_orders_dataset.csv", bucket_name=SETTINGS.data_bucket)
        return "true" if present else "false"

    run_databricks = DatabricksRunNowOperator(
        task_id="run_databricks_lakehouse_job",
        databricks_conn_id=SETTINGS.databricks_conn_id,
        job_name=SETTINGS.databricks_job_name,
        json={
            "job_parameters": {
                "run_date": "{{ ds }}",
                "mode": "daily",
                "base_uri": f"s3://{SETTINGS.data_bucket}",
                "include_olist": "{{ ti.xcom_pull(task_ids='check_olist_data') }}",
            }
        },
        polling_period_seconds=30,
        execution_timeout=timedelta(minutes=45),
        retries=1,  # a cluster failure deserves one retry; a logic failure will fail again
        outlets=[GOLD_DATASET],
    )

    cities = list_cities()
    extracted = extract_city.expand(city=cities)
    summary = validate_extraction(extracted)
    olist_flag = check_olist_data()
    [summary, olist_flag] >> run_databricks


weather_lakehouse_daily()
