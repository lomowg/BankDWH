"""Клиент ClickHouse"""

from __future__ import annotations

import os

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()


def ch_database() -> str:
    return os.getenv("CLICKHOUSE_DATABASE", "bank_marts")


def ch_client():
    raw = os.getenv("CLICKHOUSE_PASSWORD")
    password = None if raw is None or not str(raw).strip() else raw
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port_s = os.getenv("CLICKHOUSE_HTTP_PORT") or os.getenv("CLICKHOUSE_PORT", "8123")
    port = int(port_s)
    if port == 9000:
        port = 8123
    kwargs = dict(
        host=host,
        port=port,
        username=os.getenv("CLICKHOUSE_USER", "default"),
        database=ch_database(),
    )
    if password is not None:
        kwargs["password"] = password
    return clickhouse_connect.get_client(**kwargs)
