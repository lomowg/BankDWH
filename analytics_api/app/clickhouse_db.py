from __future__ import annotations

from decimal import Decimal
from typing import Any

import clickhouse_connect

from app.config import settings

_client: Any = None


def get_client() -> Any:
    global _client
    if _client is None:
        port = settings.clickhouse_http_port
        if port == 9000:
            port = 8123
        _client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password or "",
            database=settings.clickhouse_database,
        )
    return _client


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_all(sql: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    client = get_client()
    result = client.query(sql, parameters=parameters or {})
    cols = result.column_names
    out: list[dict[str, Any]] = []
    for row in result.result_rows:
        out.append({c: _json_safe(v) for c, v in zip(cols, row)})
    return out
