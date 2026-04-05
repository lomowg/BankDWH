"""
Синхронизация витрин PostgreSQL (dwh) → ClickHouse (bank_marts).

Параметры, все поля необязательны:
  {"report_date": "2026-04-04"}     — срез для mart_client_profile и mart_segment_metrics
  {"activity_from": "2026-01-01", "activity_to": "2026-04-04"}  — диапазон для дневных витрин
  {"activity_days_back": 120}       — если границы не заданы: от (report_date − N) до report_date
  
"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable, Sequence

import pendulum
import psycopg
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from bank_dwh import ch_client, ch_database, database_url
from bank_dwh.airflow_utils import activity_date_range, dag_conf, report_date_for_dag

CHUNK_ROWS = 10_000


def _ch_delete_report_date(client, table: str, rd: date) -> None:
    db = ch_database()
    client.command(
        f"ALTER TABLE {db}.{table} DELETE WHERE report_date = toDate('{rd.isoformat()}')"
    )


def _ch_delete_activity_range(client, table: str, d0: date, d1: date) -> None:
    db = ch_database()
    client.command(
        f"ALTER TABLE {db}.{table} DELETE WHERE activity_date >= toDate('{d0.isoformat()}') "
        f"AND activity_date <= toDate('{d1.isoformat()}')"
    )


def _insert_batches(
    client,
    table: str,
    column_names: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> int:
    db = ch_database()
    batch: list[Sequence[Any]] = []
    n = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= CHUNK_ROWS:
            client.insert(table, batch, column_names=column_names, database=db)
            n += len(batch)
            batch.clear()
    if batch:
        client.insert(table, batch, column_names=column_names, database=db)
        n += len(batch)
    return n


SQL_PROFILE = """
SELECT
    report_date,
    client_id,
    unified_client_key,
    client_type,
    status,
    region_code,
    active_accounts_cnt,
    active_products_cnt,
    debit_turnover_30d,
    credit_turnover_30d,
    operations_cnt_30d,
    digital_events_cnt_30d,
    appeals_cnt_90d,
    last_transaction_ts,
    last_digital_event_ts,
    current_segment_type_id
FROM dwh.client_profile_export
WHERE report_date = %s
ORDER BY client_id
"""

SQL_SEGMENT = """
SELECT
    %s::date AS report_date,
    COALESCE(e.current_segment_type_id, 0)::smallint AS segment_type_id,
    COUNT(*)::int AS clients_cnt,
    COUNT(*) FILTER (
        WHERE e.operations_cnt_30d > 0 OR e.digital_events_cnt_30d > 0
    )::int AS active_clients_30d,
    COALESCE(SUM(e.debit_turnover_30d), 0)::numeric(24, 2) AS total_debit_turnover_30d,
    COALESCE(SUM(e.credit_turnover_30d), 0)::numeric(24, 2) AS total_credit_turnover_30d,
    COALESCE(SUM(e.operations_cnt_30d), 0)::bigint AS total_operations_30d,
    CASE
        WHEN COUNT(*) > 0 THEN (SUM(e.operations_cnt_30d)::double precision / COUNT(*))::real
        ELSE 0::real
    END AS avg_operations_per_client
FROM dwh.client_profile_export e
WHERE e.report_date = %s
GROUP BY 2
ORDER BY 2
"""

SQL_ACTIVITY_DAILY = """
WITH tx AS (
    SELECT
        t.operation_date AS activity_date,
        t.client_id,
        COALESCE(ch.channel_code, 'UNKNOWN') AS channel_code,
        COUNT(*)::int AS operations_cnt,
        COALESCE(SUM(CASE WHEN t.direction = 'D' THEN t.amount ELSE 0 END), 0)::numeric(18, 2)
            AS debit_amount,
        COALESCE(SUM(CASE WHEN t.direction = 'C' THEN t.amount ELSE 0 END), 0)::numeric(18, 2)
            AS credit_amount
    FROM dwh.transactions t
    LEFT JOIN dwh.channels ch ON ch.channel_id = t.channel_id
    WHERE t.operation_date BETWEEN %s AND %s
    GROUP BY 1, 2, 3
),
ev AS (
    SELECT
        de.event_date AS activity_date,
        de.client_id,
        COALESCE(ch.channel_code, 'UNKNOWN') AS channel_code,
        COUNT(*)::int AS digital_events_cnt,
        COALESCE(SUM(CASE WHEN de.success_flag IS TRUE THEN 1 ELSE 0 END), 0)::int
            AS digital_success_cnt,
        COALESCE(SUM(CASE WHEN de.success_flag IS FALSE THEN 1 ELSE 0 END), 0)::int
            AS digital_fail_cnt
    FROM dwh.digital_events de
    LEFT JOIN dwh.channels ch ON ch.channel_id = de.channel_id
    WHERE de.event_date BETWEEN %s AND %s
    GROUP BY 1, 2, 3
),
ap AS (
    SELECT
        (a.created_ts AT TIME ZONE 'UTC')::date AS activity_date,
        a.client_id,
        COALESCE(ch.channel_code, 'UNKNOWN') AS channel_code,
        COUNT(*)::int AS appeals_opened_cnt
    FROM dwh.appeals a
    LEFT JOIN dwh.channels ch ON ch.channel_id = a.channel_id
    WHERE (a.created_ts AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
    GROUP BY 1, 2, 3
),
keys AS (
    SELECT activity_date, client_id, channel_code FROM tx
    UNION
    SELECT activity_date, client_id, channel_code FROM ev
    UNION
    SELECT activity_date, client_id, channel_code FROM ap
)
SELECT
    k.activity_date,
    k.client_id,
    k.channel_code,
    COALESCE(tx.operations_cnt, 0)::int,
    COALESCE(tx.debit_amount, 0),
    COALESCE(tx.credit_amount, 0),
    COALESCE(ev.digital_events_cnt, 0)::int,
    COALESCE(ev.digital_success_cnt, 0)::int,
    COALESCE(ev.digital_fail_cnt, 0)::int,
    COALESCE(ap.appeals_opened_cnt, 0)::int
FROM keys k
LEFT JOIN tx ON tx.activity_date = k.activity_date
    AND tx.client_id = k.client_id AND tx.channel_code = k.channel_code
LEFT JOIN ev ON ev.activity_date = k.activity_date
    AND ev.client_id = k.client_id AND ev.channel_code = k.channel_code
LEFT JOIN ap ON ap.activity_date = k.activity_date
    AND ap.client_id = k.client_id AND ap.channel_code = k.channel_code
ORDER BY k.activity_date, k.client_id, k.channel_code
"""

SQL_FINANCIAL_DAILY = """
SELECT
    t.operation_date,
    t.client_id,
    COALESCE(ch.channel_code, 'UNKNOWN'),
    COALESCE(ot.operation_type_code, 'UNKNOWN'),
    COUNT(*)::int,
    COALESCE(SUM(CASE WHEN t.direction = 'D' THEN t.amount ELSE 0 END), 0)::numeric(18, 2),
    COALESCE(SUM(CASE WHEN t.direction = 'C' THEN t.amount ELSE 0 END), 0)::numeric(18, 2)
FROM dwh.transactions t
LEFT JOIN dwh.channels ch ON ch.channel_id = t.channel_id
LEFT JOIN dwh.operation_types ot ON ot.operation_type_id = t.operation_type_id
WHERE t.operation_date BETWEEN %s AND %s
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
"""

SQL_DIGITAL_DAILY = """
SELECT
    de.event_date,
    de.client_id,
    COALESCE(ch.channel_code, 'UNKNOWN'),
    COALESCE(et.event_type_code, 'UNKNOWN'),
    COUNT(*)::int,
    COALESCE(SUM(CASE WHEN de.success_flag IS TRUE THEN 1 ELSE 0 END), 0)::int,
    COALESCE(SUM(CASE WHEN de.success_flag IS FALSE THEN 1 ELSE 0 END), 0)::int
FROM dwh.digital_events de
LEFT JOIN dwh.channels ch ON ch.channel_id = de.channel_id
LEFT JOIN dwh.event_types et ON et.event_type_id = de.event_type_id
WHERE de.event_date BETWEEN %s AND %s
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
"""

PROFILE_COLS = [
    "report_date",
    "client_id",
    "unified_client_key",
    "client_type",
    "status",
    "region_code",
    "active_accounts_cnt",
    "active_products_cnt",
    "debit_turnover_30d",
    "credit_turnover_30d",
    "operations_cnt_30d",
    "digital_events_cnt_30d",
    "appeals_cnt_90d",
    "last_transaction_ts",
    "last_digital_event_ts",
    "current_segment_type_id",
]

SEGMENT_COLS = [
    "report_date",
    "segment_type_id",
    "clients_cnt",
    "active_clients_30d",
    "total_debit_turnover_30d",
    "total_credit_turnover_30d",
    "total_operations_30d",
    "avg_operations_per_client",
]

ACTIVITY_COLS = [
    "activity_date",
    "client_id",
    "channel_code",
    "operations_cnt",
    "debit_amount",
    "credit_amount",
    "digital_events_cnt",
    "digital_success_cnt",
    "digital_fail_cnt",
    "appeals_opened_cnt",
]

FIN_COLS = [
    "activity_date",
    "client_id",
    "channel_code",
    "operation_type_code",
    "operation_cnt",
    "debit_amount",
    "credit_amount",
]

DIG_COLS = [
    "activity_date",
    "client_id",
    "channel_code",
    "event_type_code",
    "event_cnt",
    "success_cnt",
    "fail_cnt",
]


def sync_mart_client_profile_task(context: dict[str, Any]) -> int:
    conf = dag_conf(context)
    rd = report_date_for_dag(context, conf)
    ch = ch_client()
    _ch_delete_report_date(ch, "mart_client_profile", rd)
    url = database_url()
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_PROFILE, (rd,))
            return _insert_batches(ch, "mart_client_profile", PROFILE_COLS, cur.fetchall())


def sync_mart_segment_metrics_task(context: dict[str, Any]) -> int:
    conf = dag_conf(context)
    rd = report_date_for_dag(context, conf)
    ch = ch_client()
    _ch_delete_report_date(ch, "mart_segment_metrics", rd)
    url = database_url()
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_SEGMENT, (rd, rd))
            rows = cur.fetchall()
            return _insert_batches(ch, "mart_segment_metrics", SEGMENT_COLS, rows)


def sync_mart_activity_daily_task(context: dict[str, Any]) -> int:
    conf = dag_conf(context)
    d0, d1 = activity_date_range(context, conf)
    ch = ch_client()
    _ch_delete_activity_range(ch, "mart_client_activity_daily", d0, d1)
    url = database_url()
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_ACTIVITY_DAILY, (d0, d1, d0, d1, d0, d1))
            rows = cur.fetchall()
            return _insert_batches(ch, "mart_client_activity_daily", ACTIVITY_COLS, rows)


def sync_mart_financial_daily_task(context: dict[str, Any]) -> int:
    conf = dag_conf(context)
    d0, d1 = activity_date_range(context, conf)
    ch = ch_client()
    _ch_delete_activity_range(ch, "mart_financial_activity_daily", d0, d1)
    url = database_url()
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_FINANCIAL_DAILY, (d0, d1))
            rows = cur.fetchall()
            return _insert_batches(ch, "mart_financial_activity_daily", FIN_COLS, rows)


def sync_mart_digital_activity_daily_task(context: dict[str, Any]) -> int:
    conf = dag_conf(context)
    d0, d1 = activity_date_range(context, conf)
    ch = ch_client()
    _ch_delete_activity_range(ch, "mart_digital_activity_daily", d0, d1)
    url = database_url()
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_DIGITAL_DAILY, (d0, d1))
            rows = cur.fetchall()
            return _insert_batches(ch, "mart_digital_activity_daily", DIG_COLS, rows)


@dag(
    dag_id="load_marts_to_ch",
    description="PostgreSQL dwh → ClickHouse bank_marts (витрины OLAP)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dwh", "clickhouse", "postgresql"],
    doc_md=__doc__,
    default_args={
        "owner": "dwh",
        "retries": 1,
    },
)
def load_marts_to_ch():
    @task(task_id="sync_mart_client_profile")
    def sync_mart_client_profile() -> int:
        return sync_mart_client_profile_task(get_current_context())

    @task(task_id="sync_mart_segment_metrics")
    def sync_mart_segment_metrics() -> int:
        return sync_mart_segment_metrics_task(get_current_context())

    @task(task_id="sync_mart_client_activity_daily")
    def sync_mart_client_activity_daily() -> int:
        return sync_mart_activity_daily_task(get_current_context())

    @task(task_id="sync_mart_financial_activity_daily")
    def sync_mart_financial_activity_daily() -> int:
        return sync_mart_financial_daily_task(get_current_context())

    @task(task_id="sync_mart_digital_activity_daily")
    def sync_mart_digital_activity_daily() -> int:
        return sync_mart_digital_activity_daily_task(get_current_context())

    sync_mart_client_profile()
    sync_mart_segment_metrics()
    sync_mart_client_activity_daily()
    sync_mart_financial_activity_daily()
    sync_mart_digital_activity_daily()


load_marts_to_ch()
