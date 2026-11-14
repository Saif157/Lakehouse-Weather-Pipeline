from __future__ import annotations

try:  # Airflow 3
    from airflow.sdk import Asset as Dataset
    from airflow.sdk import Param, dag, task
except ImportError:  # Airflow 2.x
    from airflow.datasets import Dataset
    from airflow.decorators import dag, task
    from airflow.models.param import Param

__all__ = ["Dataset", "Param", "dag", "task"]
