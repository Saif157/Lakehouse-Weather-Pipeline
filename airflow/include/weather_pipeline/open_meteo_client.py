from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from weather_pipeline.config import HOURLY_VARIABLES

CLIENT_VERSION = "1.0"
# The Silver quality rules reject rows without these, so an all-null array is useless.
REQUIRED_VARIABLES = ("temperature_2m", "precipitation")
ENDPOINTS = {
    "forecast": "https://api.open-meteo.com/v1/forecast",
    "archive": "https://archive-api.open-meteo.com/v1/archive",
}


class ApiResponseError(RuntimeError):

def build_session(max_retries: int = 5, backoff_factor: float = 1.5) -> requests.Session:
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": f"lakehouse-weather-pipeline/{CLIENT_VERSION}"})
    return session


def expected_hours(start: date, end: date) -> int:
    return ((end - start).days + 1) * 24


def validate_payload(payload: Any, start: date, end: date,
                     variables: tuple[str, ...] = HOURLY_VARIABLES) -> None:
    if not isinstance(payload, dict):
        raise ApiResponseError("payload is not a JSON object")
    if payload.get("error"):
        raise ApiResponseError(f"API reported an error: {payload.get('reason')}")
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not hourly.get("time"):
        raise ApiResponseError("response has no hourly.time")
    n_rows = len(hourly["time"])
    wanted = expected_hours(start, end)
    if n_rows != wanted:
        raise ApiResponseError(f"expected {wanted} hourly rows, got {n_rows}")
    for variable in variables:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != n_rows:
            raise ApiResponseError(f"hourly.{variable} is missing or has the wrong length")
    for variable in REQUIRED_VARIABLES:
        if all(v is None for v in hourly[variable]):
            raise ApiResponseError(
                f"every hourly.{variable} value is null: no data is available yet for this "
                "location and date (the archive endpoint lags real time)")


class OpenMeteoClient:
    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: tuple[float, float] = (5, 30),
        min_interval_s: float = 0.25,
        endpoints: dict[str, str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.session = session or build_session()
        self.timeout = timeout
        self.min_interval_s = min_interval_s
        self.endpoints = endpoints or ENDPOINTS
        self._sleep = sleep
        self._now = now

    def fetch_hourly(self, *, city_slug: str, latitude: float, longitude: float,
                     start: date, end: date, source: str) -> dict:
        if source not in self.endpoints:
            raise ValueError(f"unknown source {source!r}; expected one of {sorted(self.endpoints)}")
        if end < start:
            raise ValueError("end must not be before start")

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "GMT",
        }
        url = self.endpoints[source]
        response = self.session.get(url, params=params, timeout=self.timeout)
        self._sleep(self.min_interval_s) 

        if 400 <= response.status_code < 500 and response.status_code != 429:
            raise ApiResponseError(
                f"HTTP {response.status_code} from {source}: {response.text[:300]}")
        response.raise_for_status()

        payload = response.json()
        validate_payload(payload, start, end)
        return {
            "meta": {
                "city": city_slug,
                "latitude": latitude,
                "longitude": longitude,
                "source": source,
                "endpoint": url,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "fetched_at": self._now().isoformat(timespec="seconds"),
                "http_status": response.status_code,
                "client_version": CLIENT_VERSION,
            },
            "payload": payload,
        }
