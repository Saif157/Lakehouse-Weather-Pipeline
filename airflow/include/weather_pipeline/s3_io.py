"""Raw-zone naming and writing. The raw zone is append-only: every fetch is a NEW object."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any


def build_raw_key(prefix: str, partition_date: date | str, city_slug: str,
                  fetched_at: datetime) -> str:

    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}/dt={partition_date}/city={city_slug}/fetched_at={stamp}.json"


def put_json(hook: Any, bucket: str, key: str, obj: dict) -> int:
    body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    hook.load_string(string_data=body, key=key, bucket_name=bucket, replace=True)
    return len(body.encode("utf-8"))
