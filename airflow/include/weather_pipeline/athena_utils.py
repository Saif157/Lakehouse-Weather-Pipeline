from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_PLACEHOLDER = re.compile(r"\{\{\s*([A-Z][A-Z0-9_]*)\s*\}\}")


def render_sql(sql: str, variables: Mapping[str, str]) -> str:
    def _sub(match: re.Match) -> str:
        name = match.group(1)
        if name not in variables:
            raise KeyError(f"SQL placeholder {{{{ {name} }}}} has no value")
        return str(variables[name])

    return _PLACEHOLDER.sub(_sub, sql)


def split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    in_quote = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_quote:
            buf.append(ch)
            if ch == "'":
                in_quote = False
        elif ch == "'":
            in_quote = True
            buf.append(ch)
        elif sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = len(sql) if newline == -1 else newline
            continue
        elif ch == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def rows_from_result(result: dict) -> list[dict[str, str | None]]:
    rows = result["ResultSet"]["Rows"]
    if not rows:
        return []
    header = [cell.get("VarCharValue") for cell in rows[0]["Data"]]
    return [
        {header[i]: cell.get("VarCharValue") for i, cell in enumerate(row["Data"])}
        for row in rows[1:]
    ]


def run_query(hook: Any, query: str, *, database: str, output_location: str, workgroup: str,
              max_polling_attempts: int = 120, sleep_time: int = 5) -> str:

    query_id = hook.run_query(
        query=query,
        query_context={"Database": database},
        result_configuration={"OutputLocation": output_location},
        workgroup=workgroup,
    )
    state = hook.poll_query_status(query_id, max_polling_attempts=max_polling_attempts,
                                   sleep_time=sleep_time)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {query_id} finished in state {state}: {query[:120]!r}")
    return query_id


def scanned_bytes(hook: Any, query_id: str) -> int:
    """Bytes Athena scanned for a finished query. This is what you are billed on."""
    info = hook.get_query_info(query_id)
    return int(info["QueryExecution"].get("Statistics", {}).get("DataScannedInBytes", 0))


def run_sql_file(hook: Any, path: Path, variables: Mapping[str, str], **kwargs: Any) -> list[str]:
    sql = render_sql(Path(path).read_text(encoding="utf-8"), variables)
    return [run_query(hook, statement, **kwargs) for statement in split_statements(sql)]
