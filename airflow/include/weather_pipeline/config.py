from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta

HOURLY_VARIABLES = ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m")

RAW_PREFIX = "raw/open_meteo"
GOLD_PREFIX = "gold/daily_city_weather"
OLIST_PREFIX = "raw/olist"

# Recent days come from the forecast endpoint (past data is available there for up to 92 days);
# older days come from the archive endpoint (ERA5 reanalysis), which Open-Meteo documents as
# "daily with 5 days delay". 10 days leaves a safety margin on top of that. The rule lives in ONE
# place so the daily DAG, the backfill DAG and the tests agree.
RECENT_WINDOW_DAYS = 10


@dataclass(frozen=True)
class City:
    slug: str  
    name: str
    latitude: float
    longitude: float
    olist_key: str  

    def as_dict(self) -> dict:
        return asdict(self)


CITIES: tuple[City, ...] = (
    City("sao_paulo", "São Paulo", -23.5505, -46.6333, "sao paulo"),
    City("rio_de_janeiro", "Rio de Janeiro", -22.9068, -43.1729, "rio de janeiro"),
    City("belo_horizonte", "Belo Horizonte", -19.9167, -43.9345, "belo horizonte"),
    City("brasilia", "Brasília", -15.7939, -47.8828, "brasilia"),
    City("curitiba", "Curitiba", -25.4284, -49.2733, "curitiba"),
    City("campinas", "Campinas", -22.9099, -47.0626, "campinas"),
    City("porto_alegre", "Porto Alegre", -30.0346, -51.2177, "porto alegre"),
    City("salvador", "Salvador", -12.9777, -38.5016, "salvador"),
    City("guarulhos", "Guarulhos", -23.4543, -46.5337, "guarulhos"),
    City("sao_bernardo_do_campo", "São Bernardo do Campo", -23.6914, -46.5646,
         "sao bernardo do campo"),
)


def city_by_slug(slug: str) -> City:
    for city in CITIES:
        if city.slug == slug:
            return city
    raise KeyError(f"unknown city slug: {slug!r}")


def pick_source(target: date, today: date) -> str:
    """Which API endpoint should serve `target`, as seen from `today`."""
    return "forecast" if (today - target).days <= RECENT_WINDOW_DAYS else "archive"


def iter_chunks(start: date, end: date, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Split [start, end] (inclusive) into consecutive windows of at most `chunk_days` days."""
    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")
    if end < start:
        raise ValueError("end must not be before start")
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


@dataclass(frozen=True)
class Settings:
    data_bucket: str
    aws_conn_id: str = "aws_default"
    databricks_conn_id: str = "databricks_default"
    databricks_job_name: str = "weather_lakehouse"
    athena_database: str = "weather_analytics"
    athena_workgroup: str = "weather-pipeline"
    athena_output: str = ""
    athena_sql_dir: str = "/opt/airflow/athena"
    alert_topic_arn: str | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        bucket = env.get("DATA_BUCKET", "your-lakehouse-bucket")
        return cls(
            data_bucket=bucket,
            aws_conn_id=env.get("AWS_CONN_ID", "aws_default"),
            databricks_conn_id=env.get("DATABRICKS_CONN_ID", "databricks_default"),
            databricks_job_name=env.get("DATABRICKS_JOB_NAME", "weather_lakehouse"),
            athena_database=env.get("ATHENA_DATABASE", "weather_analytics"),
            athena_workgroup=env.get("ATHENA_WORKGROUP", "weather-pipeline"),
            athena_output=env.get("ATHENA_OUTPUT", f"s3://{bucket}/athena-results/"),
            athena_sql_dir=env.get("ATHENA_SQL_DIR", "/opt/airflow/athena"),
            alert_topic_arn=env.get("ALERT_SNS_TOPIC_ARN") or None,
        )
