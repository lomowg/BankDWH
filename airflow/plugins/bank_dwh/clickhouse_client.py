"""Клиент ClickHouse"""

from __future__ import annotations

import os

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()


def ch_database() -> str:
    return os.getenv("CLICKHOUSE_DATABASE", "bank_marts")


def ch_client():
    password = os.getenv("CLICKHOUSE_PASSWORD")
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port_s = os.getenv("CLICKHOUSE_HTTP_PORT") or os.getenv("CLICKHOUSE_PORT", "8123")
    port = int(port_s)
    if port == 9000:
        port = 8123
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password="" if password is None else password,
        database=ch_database(),
    )
