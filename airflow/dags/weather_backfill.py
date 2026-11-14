from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from weather_pipeline.airflow_compat import Param, dag, task
from weather_pipeline.alerts import notify_failure
from weather_pipeline.config import (
    CITIES,
    OLIST_PREFIX,
    RAW_PREFIX,
    RECENT_WINDOW_DAYS,
    Settings,
    city_by_slug,
    iter_chunks,
)
from weather_pipeline.open_meteo_client import ApiResponseError, OpenMeteoClient
from weather_pipeline.s3_io import build_raw_key, put_json
from weather_pipeline.weather_datasets import GOLD_DATASET

log = logging.getLogger(__name__)
SETTINGS = Settings.from_env()


@dag(
    dag_id="weather_backfill",
    description="Historical weather backfill (dynamic mapping), then a Databricks backfill run",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2017-01-01", type="string", format="date"),
        "end_date": Param("2018-08-31", type="string", format="date"),
        "cities": Param([c.slug for c in CITIES], type="array", items={"type": "string"}),
        "chunk_days": Param(31, type="integer", minimum=1, maximum=366),
    },
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": notify_failure,
    },
    tags=["lakehouse", "backfill", "rest-api", "databricks"],
    doc_md=__doc__,
)
def weather_backfill():
    @task
    def plan_requests(params=None) -> list[dict]:
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"])
        if end < start:
            raise AirflowFailException("end_date is before start_date")
        if (date.today() - end).days <= RECENT_WINDOW_DAYS:
            raise AirflowFailException(
                f"end_date must be more than {RECENT_WINDOW_DAYS} days ago; "
                "recent days belong to the daily DAG")
        try:
            cities = [city_by_slug(slug) for slug in params["cities"]]
        except KeyError as exc:
            raise AirflowFailException(str(exc)) from exc
        requests_ = [
            {"city": city.as_dict(), "start": s.isoformat(), "end": e.isoformat()}
            for city in cities
            for s, e in iter_chunks(start, end, int(params["chunk_days"]))
        ]
        log.info("Planned %d requests for %d cities", len(requests_), len(cities))
        return requests_

    @task(pool="open_meteo_api", max_active_tis_per_dag=4, retries=4,
          execution_timeout=timedelta(minutes=10))
    def extract_chunk(request: dict) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        city = request["city"]
        start, end = date.fromisoformat(request["start"]), date.fromisoformat(request["end"])
        try:
            envelope = OpenMeteoClient().fetch_hourly(
                city_slug=city["slug"], latitude=city["latitude"], longitude=city["longitude"],
                start=start, end=end, source="archive")
        except ApiResponseError as exc:
            raise AirflowFailException(f"{city['slug']} {start}..{end}: {exc}") from exc
        fetched_at = datetime.fromisoformat(envelope["meta"]["fetched_at"])
        key = build_raw_key(RAW_PREFIX, start, city["slug"], fetched_at)  # dt = first day in file
        hook = S3Hook(aws_conn_id=SETTINGS.aws_conn_id)
        size = put_json(hook, SETTINGS.data_bucket, key, envelope)
        return {"city": city["slug"], "key": key,
                "hours": len(envelope["payload"]["hourly"]["time"]), "bytes": size}

    @task
    def summarize(results: list[dict]) -> dict:
        results = list(results)
        summary = {"requests": len(results), "hours": sum(r["hours"] for r in results),
                   "bytes": sum(r["bytes"] for r in results)}
        log.info("Backfill extraction finished: %s", summary)
        return summary

    @task
    def check_olist_data() -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        present = S3Hook(aws_conn_id=SETTINGS.aws_conn_id).check_for_key(
            f"{OLIST_PREFIX}/olist_orders_dataset.csv", bucket_name=SETTINGS.data_bucket)
        return "true" if present else "false"

    run_databricks = DatabricksRunNowOperator(
        task_id="run_databricks_backfill",
        databricks_conn_id=SETTINGS.databricks_conn_id,
        job_name=SETTINGS.databricks_job_name,
        json={
            "job_parameters": {
                "mode": "backfill",
                "start_date": "{{ params.start_date }}",
                "end_date": "{{ params.end_date }}",
                "base_uri": f"s3://{SETTINGS.data_bucket}",
                "include_olist": "{{ ti.xcom_pull(task_ids='check_olist_data') }}",
            }
        },
        polling_period_seconds=30,
        execution_timeout=timedelta(hours=2),
        retries=0,
        outlets=[GOLD_DATASET],
    )

    planned = plan_requests()
    extracted = extract_chunk.expand(request=planned)
    done = summarize(extracted)
    [done, check_olist_data()] >> run_databricks


weather_backfill()
