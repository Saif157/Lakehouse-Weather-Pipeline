from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException

from weather_pipeline.airflow_compat import dag, task
from weather_pipeline.alerts import notify_failure
from weather_pipeline.athena_utils import rows_from_result, run_query, run_sql_file, scanned_bytes
from weather_pipeline.config import CITIES, Settings
from weather_pipeline.weather_datasets import GOLD_DATASET

log = logging.getLogger(__name__)

SETTINGS = Settings.from_env()
MAX_SCAN_BYTES = 50 * 1024 * 1024 

def _hook():
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook

    return AthenaHook(aws_conn_id=SETTINGS.aws_conn_id)


def _query_kwargs() -> dict:
    return {
        "database": SETTINGS.athena_database,
        "output_location": SETTINGS.athena_output,
        "workgroup": SETTINGS.athena_workgroup,
    }


@dag(
    dag_id="weather_athena_analytics",
    description="Create Athena objects and validate the newest Gold partition",
    schedule=[GOLD_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    tags=["lakehouse", "athena", "data-quality"],
    doc_md=__doc__,
)
def weather_athena_analytics():
    @task
    def ensure_catalog_objects() -> int:
        """CREATE ... IF NOT EXISTS for the database and tables (safe to run every time)."""
        variables = {
            "BUCKET": SETTINGS.data_bucket,
            "DATABASE": SETTINGS.athena_database,
            "CITY_VALUES": ",".join(city.slug for city in CITIES),
        }
        hook = _hook()
        ran = 0
        for name in ("01_create_database.sql", "02_create_daily_city_weather.sql",
                     "03_create_delivery_weather_impact.sql"):
            ids = run_sql_file(
                hook, Path(SETTINGS.athena_sql_dir) / name, variables,
                database="default", output_location=SETTINGS.athena_output,
                workgroup=SETTINGS.athena_workgroup)
            ran += len(ids)
        log.info("Ran %d DDL statements", ran)
        return ran

    @task
    def validate_latest_partition(_ddl_count: int) -> dict:
        """Each city needs a complete day in the newest partition, and the check must stay cheap."""
        hook = _hook()
        years = (datetime.now(UTC).year - 1, datetime.now(UTC).year)
        table = f"{SETTINGS.athena_database}.daily_city_weather"
        latest_id = run_query(
            hook,
            f"SELECT CAST(max(date_utc) AS varchar) AS latest FROM {table} "
            f"WHERE year IN ({years[0]}, {years[1]})",
            **_query_kwargs())
        latest_rows = rows_from_result(hook.get_query_results(latest_id))
        latest = latest_rows[0]["latest"] if latest_rows else None
        if not latest:
            raise AirflowFailException("No rows found in the Gold table for the last two years")

        year, month = int(latest[:4]), int(latest[5:7])
        day_id = run_query(
            hook,
            f"SELECT city, hours_observed FROM {table} "
            f"WHERE year = {year} AND month = {month} AND date_utc = DATE '{latest}'",
            **_query_kwargs())
        rows = rows_from_result(hook.get_query_results(day_id))

        problems = []
        got = {row["city"] for row in rows}
        expected = {city.slug for city in CITIES}
        if got != expected:
            problems.append(f"cities missing for {latest}: {sorted(expected - got)}")
        incomplete = sorted(r["city"] for r in rows if int(r["hours_observed"] or 0) != 24)
        if incomplete:
            problems.append(f"days without 24 hours: {incomplete}")

        scanned = scanned_bytes(hook, latest_id) + scanned_bytes(hook, day_id)
        log.info("Latest Gold date %s: %d cities, %d bytes scanned", latest, len(rows), scanned)
        if scanned > MAX_SCAN_BYTES:
            problems.append(f"validation scanned {scanned} bytes (budget {MAX_SCAN_BYTES}); "
                            "check partition filters")
        if problems:
            raise AirflowFailException("; ".join(problems))
        return {"latest_date": latest, "cities": len(rows), "scanned_bytes": scanned}

    validate_latest_partition(ensure_catalog_objects())


weather_athena_analytics()
