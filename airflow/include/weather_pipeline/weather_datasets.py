"""The Dataset (Airflow 3: Asset) that links the producers (daily + backfill) to the consumer."""
from __future__ import annotations

from weather_pipeline.airflow_compat import Dataset
from weather_pipeline.config import GOLD_PREFIX, Settings

GOLD_DATASET = Dataset(f"s3://{Settings.from_env().data_bucket}/{GOLD_PREFIX}")
