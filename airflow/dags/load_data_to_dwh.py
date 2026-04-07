"""
Синхронизация источников (CSV) -> PostgreSQL: stg -> dwh -> client_profile_export.

Источники:
  - data/sample (или ETL_SAMPLE_DATA_DIR)
  - data/datasets/ (или ETL_DATASETS_DIR)
  - data/datasets/manifest.json — additional_loaders для внешних CSV и column_map

Параметры:
  {"reset": true} - очистка таблиц (TRUNCATE stg/dwh фактов)
  {"report_date": "2026-04-01"}  — отчётная дата для витрины
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pendulum
import psycopg
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from bank_dwh import database_url, datasets_data_dir, sample_data_dir
from bank_dwh.airflow_utils import dag_conf, report_date_for_dag
from psycopg.types.json import Json

_DAGS_DIR = Path(__file__).resolve().parent


def _read_csv(path: Path) -> list[dict[str, Any]]:
    """Читает CSV в Unicode: UTF-8 (с BOM), UTF-16 LE/BE, иначе UTF-8 или cp1251"""
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        text = raw.decode("utf-16-le")
    elif raw.startswith(b"\xfe\xff"):
        text = raw.decode("utf-16-be")
    else:
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1251")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def _normalize_manifest_row(table: str, row: dict[str, Any]) -> None:
    """Приведение значений из внешних датасетов к ожиданиям DWH."""
    if table == "stg.abs_clients_raw":
        cid = row.get("source_client_id")
        if cid is not None and cid != "":
            row["source_client_id"] = str(cid).strip()
        ct = row.get("client_type")
        if ct:
            u = str(ct).strip().upper()
            row["client_type"] = "BUSINESS" if u == "BUSINESS" else "INDIVIDUAL"
        if not row.get("status"):
            row["status"] = "ACTIVE"
    elif table == "stg.abs_transactions_raw":
        for k in ("source_transaction_id", "source_client_id", "source_account_id"):
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                row[k] = str(v).strip()
        if not row.get("channel_code"):
            row["channel_code"] = "ONLINE"
        if not row.get("currency_code"):
            row["currency_code"] = "INR"
        if not row.get("direction"):
            row["direction"] = "D"
        if not row.get("status"):
            row["status"] = "POSTED"


def _insert_stg_rows(
    cur: psycopg.Cursor,
    batch_id: int,
    table: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    col_list = ", ".join(["batch_id", *columns])
    placeholders = ", ".join(["%s"] * (1 + len(columns)))
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    for r in rows:
        values: list[Any] = [batch_id]
        for c in columns:
            v = r.get(c)
            values.append(v if v != "" else None)
        cur.execute(sql, values)


def _load_manifest_extra(
    cur: psycopg.Cursor,
    batch_id: int,
    manifest_dir: Path,
) -> None:
    man_path = manifest_dir / "manifest.json"
    if not man_path.is_file():
        return
    doc = json.loads(man_path.read_text(encoding="utf-8"))
    base = manifest_dir.resolve()
    for spec in doc.get("additional_loaders", []):
        rel = spec.get("file")
        table = spec.get("table")
        cols = spec.get("columns")
        if not rel or not table or not cols:
            continue
        columns = tuple(str(c) for c in cols)
        path = (manifest_dir / rel).resolve()
        try:
            path.relative_to(base)
        except ValueError:
            continue
        if not path.is_file():
            continue
        raw_rows = _read_csv(path)
        if not raw_rows:
            continue
        cmap = spec.get("column_map")
        if cmap:
            norm: list[dict[str, Any]] = []
            for r in raw_rows:
                row_out: dict[str, Any] = {c: None for c in columns}
                for csv_header, stg_col in cmap.items():
                    if stg_col not in row_out:
                        continue
                    v = r.get(csv_header)
                    row_out[stg_col] = v if v not in ("", None) else None
                _normalize_manifest_row(table, row_out)
                norm.append(row_out)
            _insert_stg_rows(cur, batch_id, table, columns, norm)
        else:
            for r in raw_rows:
                _normalize_manifest_row(table, r)
            _insert_stg_rows(cur, batch_id, table, columns, raw_rows)


def load_all_csv(
    conn: psycopg.Connection,
    batch_id: int,
    data_roots: list[Path],
    manifest_dir: Path | None = None,
) -> None:
    loaders: list[tuple[str, str, tuple[str, ...]]] = [
        ("abs_clients.csv", "stg.abs_clients_raw", (
            "source_client_id", "client_type", "first_name", "last_name", "middle_name",
            "birth_date", "registration_date", "tax_id", "phone", "email", "region_code", "status",
        )),
        ("crm_clients.csv", "stg.crm_clients_raw", (
            "source_client_id", "tax_id", "phone", "email", "region_code", "loyalty_tier",
        )),
        ("abs_accounts.csv", "stg.abs_accounts_raw", (
            "source_account_id", "source_client_id", "account_number_masked", "account_type",
            "currency_code", "opened_at", "closed_at", "status",
        )),
        ("abs_products.csv", "stg.abs_products_raw", (
            "source_product_id", "source_client_id", "source_account_id", "product_type_code",
            "product_name", "currency_code", "opened_at", "closed_at", "status", "amount_limit", "interest_rate",
        )),
        ("abs_transactions.csv", "stg.abs_transactions_raw", (
            "source_transaction_id", "source_client_id", "source_account_id", "source_product_id",
            "channel_code", "operation_type_code", "operation_ts", "amount", "currency_code",
            "direction", "status", "merchant_name", "description",
        )),
        ("dbo_events.csv", "stg.dbo_events_raw", (
            "source_event_id", "source_client_id", "channel_code", "event_type_code", "event_ts",
            "device_type", "platform", "session_id", "success_flag", "payload_json",
        )),
        ("appeals.csv", "stg.appeals_raw", (
            "source_appeal_id", "source_client_id", "channel_code", "appeal_type_code",
            "created_ts", "resolved_ts", "status", "priority", "result_text",
        )),
    ]

    with conn.cursor() as cur:
        for filename, table, columns in loaders:
            merged: list[dict[str, Any]] = []
            for root in data_roots:
                p = root / filename
                if p.is_file():
                    merged.extend(_read_csv(p))
            if merged:
                _insert_stg_rows(cur, batch_id, table, columns, merged)

        if manifest_dir and manifest_dir.is_dir():
            _load_manifest_extra(cur, batch_id, manifest_dir)


ABS_SOURCE_ID = 1
CRM_SOURCE_ID = 2
DBO_SOURCE_ID = 3


@dataclass
class IdAllocator:
    conn: psycopg.Connection

    def next(self, sql: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) + 1 if row and row[0] is not None else 1


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    return date.fromisoformat(s[:10])


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_decimal(s: str | None) -> Decimal | None:
    if not s or not str(s).strip():
        return None
    return Decimal(str(s).replace(",", "."))


def _bool_from_str(s: str | None) -> bool | None:
    if s is None or s == "":
        return None
    return str(s).lower() in ("1", "true", "yes", "t")


def transform_batch(conn: psycopg.Connection, batch_id: int) -> None:
    alloc_client = IdAllocator(conn)
    alloc_account = IdAllocator(conn)
    alloc_product = IdAllocator(conn)
    alloc_txn = IdAllocator(conn)
    alloc_event = IdAllocator(conn)
    alloc_appeal = IdAllocator(conn)
    alloc_hist = IdAllocator(conn)

    start_client = alloc_client.next("SELECT COALESCE(MAX(client_id), 0) FROM dwh.clients")
    start_account = alloc_account.next("SELECT COALESCE(MAX(account_id), 0) FROM dwh.accounts")
    start_product = alloc_product.next("SELECT COALESCE(MAX(product_id), 0) FROM dwh.products")
    start_txn = alloc_txn.next("SELECT COALESCE(MAX(transaction_id), 0) FROM dwh.transactions")
    start_event = alloc_event.next("SELECT COALESCE(MAX(digital_event_id), 0) FROM dwh.digital_events")
    start_appeal = alloc_appeal.next("SELECT COALESCE(MAX(appeal_id), 0) FROM dwh.appeals")
    start_hist = alloc_hist.next("SELECT COALESCE(MAX(client_history_id), 0) FROM dwh.client_history")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_client_id, client_type, first_name, last_name, middle_name,
                   birth_date, registration_date, tax_id, phone, email, region_code, status
            FROM stg.abs_clients_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        abs_rows = cur.fetchall()

        cur.execute(
            """
            SELECT source_client_id, tax_id, phone, email, region_code
            FROM stg.crm_clients_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        crm_rows = cur.fetchall()

    tax_to_client: dict[str, int] = {}
    source_to_client: dict[str, int] = {}
    now = datetime.now(timezone.utc)
    hist_id = start_hist - 1

    with conn.cursor() as cur:
        cid = start_client
        for row in abs_rows:
            (
                source_client_id,
                client_type,
                first_name,
                last_name,
                middle_name,
                birth_date_s,
                registration_date_s,
                tax_id,
                phone,
                email,
                region_code,
                status,
            ) = row
            tax_key = (tax_id or "").strip()
            if tax_key and tax_key in tax_to_client:
                client_id = tax_to_client[tax_key]
                cur.execute(
                    """
                    INSERT INTO dwh.client_source_map (
                        source_system_id, source_client_id, client_id, is_primary, match_rule, matched_at
                    ) VALUES (%s, %s, %s, false, 'ABS_DEDUP_TAX', %s)
                    ON CONFLICT (source_system_id, source_client_id) DO UPDATE SET
                        client_id = EXCLUDED.client_id,
                        match_rule = EXCLUDED.match_rule,
                        matched_at = EXCLUDED.matched_at
                    """,
                    (ABS_SOURCE_ID, source_client_id, client_id, now),
                )
            else:
                client_id = cid
                cid += 1
                unified_key = uuid4()
                if tax_key:
                    tax_to_client[tax_key] = client_id
                birth_date = _parse_date(birth_date_s)
                registration_date = _parse_date(registration_date_s)
                full_name = " ".join(p for p in [last_name, first_name, middle_name] if p).strip() or None
                cur.execute(
                    """
                    INSERT INTO dwh.clients (
                        client_id, unified_client_key, client_type, first_name, last_name, middle_name,
                        full_name, birth_date, registration_date, tax_id, phone, email, region_code,
                        status, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        client_id,
                        unified_key,
                        client_type or "INDIVIDUAL",
                        first_name,
                        last_name,
                        middle_name,
                        full_name,
                        birth_date,
                        registration_date,
                        tax_id,
                        phone,
                        email,
                        region_code,
                        status or "ACTIVE",
                        now,
                        now,
                    ),
                )
                hist_id += 1
                cur.execute(
                    """
                    INSERT INTO dwh.client_history (
                        client_history_id, client_id, full_name, phone, email, region_code, status,
                        effective_from, effective_to, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, true)
                    """,
                    (hist_id, client_id, full_name, phone, email, region_code, status or "ACTIVE", now),
                )
                cur.execute(
                    """
                    INSERT INTO dwh.client_source_map (
                        source_system_id, source_client_id, client_id, is_primary, match_rule, matched_at
                    ) VALUES (%s, %s, %s, true, 'ABS_TAX_KEY', %s)
                    ON CONFLICT (source_system_id, source_client_id) DO UPDATE SET
                        client_id = EXCLUDED.client_id,
                        is_primary = EXCLUDED.is_primary,
                        match_rule = EXCLUDED.match_rule,
                        matched_at = EXCLUDED.matched_at
                    """,
                    (ABS_SOURCE_ID, source_client_id, client_id, now),
                )
            source_to_client[str(source_client_id)] = client_id

        for row in crm_rows:
            source_client_id, tax_id, phone, email, region_code = row
            tax_key = (tax_id or "").strip()
            client_id = tax_to_client.get(tax_key)
            if client_id is None:
                client_id = cid
                cid += 1
                unified_key = uuid4()
                if tax_key:
                    tax_to_client[tax_key] = client_id
                cur.execute(
                    """
                    INSERT INTO dwh.clients (
                        client_id, unified_client_key, client_type, first_name, last_name, middle_name,
                        full_name, birth_date, registration_date, tax_id, phone, email, region_code,
                        status, created_at, updated_at
                    ) VALUES (
                        %s, %s, 'INDIVIDUAL', NULL, NULL, NULL, NULL, NULL, NULL, %s, %s, %s, %s,
                        'ACTIVE', %s, %s
                    )
                    """,
                    (client_id, unified_key, tax_id, phone, email, region_code, now, now),
                )
                hist_id += 1
                cur.execute(
                    """
                    INSERT INTO dwh.client_history (
                        client_history_id, client_id, full_name, phone, email, region_code, status,
                        effective_from, effective_to, is_current
                    ) VALUES (%s, %s, NULL, %s, %s, %s, 'ACTIVE', %s, NULL, true)
                    """,
                    (hist_id, client_id, phone, email, region_code, now),
                )
            cur.execute(
                """
                INSERT INTO dwh.client_source_map (
                    source_system_id, source_client_id, client_id, is_primary, match_rule, matched_at
                ) VALUES (%s, %s, %s, false, 'CRM_TAX_MATCH', %s)
                ON CONFLICT (source_system_id, source_client_id) DO UPDATE SET
                    client_id = EXCLUDED.client_id,
                    matched_at = EXCLUDED.matched_at
                """,
                (CRM_SOURCE_ID, source_client_id, client_id, now),
            )
            source_to_client[f"CRM:{source_client_id}"] = client_id

        cur.execute(
            "SELECT product_type_code, product_type_id FROM dwh.product_types",
        )
        product_type_by_code = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("SELECT channel_code, channel_id FROM dwh.channels")
        channel_by_code = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("SELECT operation_type_code, operation_type_id FROM dwh.operation_types")
        op_type_by_code = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("SELECT event_type_code, event_type_id FROM dwh.event_types")
        event_type_by_code = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("SELECT appeal_type_code, appeal_type_id FROM dwh.appeal_types")
        appeal_type_by_code = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT source_account_id, source_client_id, account_number_masked, account_type,
                   currency_code, opened_at, closed_at, status
            FROM stg.abs_accounts_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        account_rows = cur.fetchall()

    account_map: dict[str, int] = {}
    aid = start_account
    with conn.cursor() as cur:
        for row in account_rows:
            (
                source_account_id,
                source_client_id,
                account_number_masked,
                account_type,
                currency_code,
                opened_at_s,
                closed_at_s,
                status,
            ) = row
            client_id = source_to_client.get(str(source_client_id))
            if client_id is None:
                continue
            account_id = aid
            aid += 1
            account_map[str(source_account_id)] = account_id
            cur.execute(
                """
                INSERT INTO dwh.accounts (
                    account_id, source_system_id, source_account_id, client_id, account_number_masked,
                    account_type, currency_code, opened_at, closed_at, status, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system_id, source_account_id) DO UPDATE SET
                    client_id = EXCLUDED.client_id,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
                RETURNING account_id
                """,
                (
                    account_id,
                    ABS_SOURCE_ID,
                    source_account_id,
                    client_id,
                    account_number_masked,
                    account_type,
                    (currency_code or "RUB")[:3],
                    _parse_date(opened_at_s),
                    _parse_date(closed_at_s),
                    status or "ACTIVE",
                    now,
                    now,
                ),
            )
            real_id = cur.fetchone()[0]
            account_map[str(source_account_id)] = real_id

    pid = start_product
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_product_id, source_client_id, source_account_id, product_type_code,
                   product_name, currency_code, opened_at, closed_at, status, amount_limit, interest_rate
            FROM stg.abs_products_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        product_rows = cur.fetchall()

    product_map: dict[str, int] = {}
    with conn.cursor() as cur:
        for row in product_rows:
            (
                source_product_id,
                source_client_id,
                source_account_id,
                product_type_code,
                product_name,
                currency_code,
                opened_at_s,
                closed_at_s,
                status,
                amount_limit_s,
                interest_rate_s,
            ) = row
            client_id = source_to_client.get(str(source_client_id))
            if client_id is None:
                continue
            default_pt = next(iter(product_type_by_code.values()), 1)
            ptype_id = product_type_by_code.get((product_type_code or "").strip(), default_pt)
            account_id = account_map.get(str(source_account_id)) if source_account_id else None
            product_id = pid
            pid += 1
            cur.execute(
                """
                INSERT INTO dwh.products (
                    product_id, source_system_id, source_product_id, client_id, account_id, product_type_id,
                    product_name, currency_code, opened_at, closed_at, status, amount_limit, interest_rate,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system_id, source_product_id) DO UPDATE SET
                    client_id = EXCLUDED.client_id,
                    account_id = EXCLUDED.account_id,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
                RETURNING product_id
                """,
                (
                    product_id,
                    ABS_SOURCE_ID,
                    source_product_id,
                    client_id,
                    account_id,
                    ptype_id,
                    product_name,
                    (currency_code or "RUB")[:3] if currency_code else None,
                    _parse_date(opened_at_s),
                    _parse_date(closed_at_s),
                    status or "ACTIVE",
                    _parse_decimal(amount_limit_s),
                    _parse_decimal(interest_rate_s),
                    now,
                    now,
                ),
            )
            real_pid = cur.fetchone()[0]
            product_map[str(source_product_id)] = real_pid

    tid = start_txn
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_transaction_id, source_client_id, source_account_id, source_product_id,
                   channel_code, operation_type_code, operation_ts, amount, currency_code,
                   direction, status, merchant_name, description
            FROM stg.abs_transactions_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        txn_rows = cur.fetchall()

        for row in txn_rows:
            (
                source_transaction_id,
                source_client_id,
                source_account_id,
                source_product_id,
                channel_code,
                operation_type_code,
                operation_ts_s,
                amount_s,
                currency_code,
                direction,
                status,
                merchant_name,
                description,
            ) = row
            client_id = source_to_client.get(str(source_client_id))
            if client_id is None:
                continue
            account_id = account_map.get(str(source_account_id)) if source_account_id else None
            product_id = product_map.get(str(source_product_id)) if source_product_id else None
            channel_id = channel_by_code.get(channel_code or "", None)
            op_id = op_type_by_code.get(operation_type_code or "", None)
            op_ts = _parse_ts(operation_ts_s)
            if op_ts is None:
                continue
            op_date = op_ts.date()
            amt = _parse_decimal(amount_s) or Decimal("0")
            transaction_id = tid
            tid += 1
            cur.execute(
                """
                INSERT INTO dwh.transactions (
                    transaction_id, source_system_id, source_transaction_id, client_id, account_id,
                    product_id, channel_id, operation_type_id, operation_ts, operation_date, amount,
                    currency_code, direction, status, merchant_name, description, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system_id, source_transaction_id) DO UPDATE SET
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status
                """,
                (
                    transaction_id,
                    ABS_SOURCE_ID,
                    source_transaction_id,
                    client_id,
                    account_id,
                    product_id,
                    channel_id,
                    op_id,
                    op_ts,
                    op_date,
                    amt,
                    (currency_code or "RUB")[:3],
                    direction or "D",
                    status,
                    merchant_name,
                    description,
                    now,
                ),
            )

        cur.execute(
            """
            SELECT source_event_id, source_client_id, channel_code, event_type_code, event_ts,
                   device_type, platform, session_id, success_flag, payload_json
            FROM stg.dbo_events_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        event_rows = cur.fetchall()

        eid = start_event
        for row in event_rows:
            (
                source_event_id,
                source_client_id,
                channel_code,
                event_type_code,
                event_ts_s,
                device_type,
                platform,
                session_id,
                success_flag_s,
                payload_json,
            ) = row
            client_id = source_to_client.get(str(source_client_id))
            if client_id is None:
                continue
            ch_code = (channel_code or "").strip()
            et_code = (event_type_code or "").strip()
            channel_id = channel_by_code.get(ch_code)
            et_id = event_type_by_code.get(et_code)
            if channel_id is None or et_id is None:
                continue
            ev_ts = _parse_ts(event_ts_s)
            if ev_ts is None:
                continue
            try:
                payload_obj: Any = json.loads(payload_json) if payload_json else {}
            except json.JSONDecodeError:
                payload_obj = {}
            cur.execute(
                """
                INSERT INTO dwh.digital_events (
                    digital_event_id, source_system_id, source_event_id, client_id, channel_id, event_type_id,
                    event_ts, event_date, device_type, platform, session_id, success_flag, payload, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system_id, source_event_id) DO UPDATE SET
                    success_flag = EXCLUDED.success_flag
                """,
                (
                    eid,
                    DBO_SOURCE_ID,
                    source_event_id,
                    client_id,
                    channel_id,
                    et_id,
                    ev_ts,
                    ev_ts.date(),
                    device_type,
                    platform,
                    session_id,
                    _bool_from_str(success_flag_s),
                    Json(payload_obj),
                    now,
                ),
            )
            eid += 1

        apid = start_appeal
        cur.execute(
            """
            SELECT source_appeal_id, source_client_id, channel_code, appeal_type_code,
                   created_ts, resolved_ts, status, priority, result_text
            FROM stg.appeals_raw WHERE batch_id = %s
            ORDER BY id
            """,
            (batch_id,),
        )
        appeal_rows = cur.fetchall()

        for row in appeal_rows:
            (
                source_appeal_id,
                source_client_id,
                channel_code,
                appeal_type_code,
                created_ts_s,
                resolved_ts_s,
                status,
                priority_s,
                result_text,
            ) = row
            client_id = source_to_client.get(str(source_client_id))
            if client_id is None:
                continue
            channel_id = channel_by_code.get(channel_code or "", None)
            at_id = appeal_type_by_code.get(appeal_type_code or "", None)
            c_ts = _parse_ts(created_ts_s)
            if c_ts is None:
                continue
            r_ts = _parse_ts(resolved_ts_s)
            pr: int | None = None
            if priority_s and str(priority_s).strip().isdigit():
                pr = int(priority_s)
            cur.execute(
                """
                INSERT INTO dwh.appeals (
                    appeal_id, source_system_id, source_appeal_id, client_id, channel_id, appeal_type_id,
                    created_ts, resolved_ts, status, priority, result_text, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system_id, source_appeal_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    resolved_ts = EXCLUDED.resolved_ts
                """,
                (
                    apid,
                    ABS_SOURCE_ID,
                    source_appeal_id,
                    client_id,
                    channel_id,
                    at_id,
                    c_ts,
                    r_ts,
                    status or "OPEN",
                    pr,
                    result_text,
                    now,
                ),
            )
            apid += 1

def recompute_client_segments(conn: psycopg.Connection, report_date: date) -> None:
    """
    NEW - дата регистрации/создания клиента в пределах 90 дней до отчёта.
    DORMANT - не новый и нет операций/событий 180+ дней (последняя активность по max(транзакция, ДБО)).
    HIGH_VALUE - высокая активность или портфель: оборот 30д >= 250k, или >= 12 операций, или >= 4 активных продукта.
    ACTIVE - умеренная активность 30д: >= 3 операций, или >= 6 цифровых событий, или оборот >= 15k.
    LOW_ACTIVITY - остальные клиенты
    """
    delete_sql = "DELETE FROM dwh.client_segments_history WHERE assigned_by = 'ETL_SEGMENTATION'"
    insert_sql = """
    WITH tx30 AS (
        SELECT
            client_id,
            COUNT(*)::int AS op_30,
            COALESCE(SUM(CASE WHEN direction = 'D' THEN amount ELSE 0 END), 0) AS debit_30,
            COALESCE(SUM(CASE WHEN direction = 'C' THEN amount ELSE 0 END), 0) AS credit_30
        FROM dwh.transactions
        WHERE operation_date >= %(rd)s::date - INTERVAL '30 days'
          AND operation_date <= %(rd)s::date
        GROUP BY client_id
    ),
    last_any AS (
        SELECT client_id, MAX(operation_ts) AS last_tx_ts
        FROM dwh.transactions
        GROUP BY client_id
    ),
    last_dig AS (
        SELECT client_id, MAX(event_ts) AS last_ev_ts
        FROM dwh.digital_events
        GROUP BY client_id
    ),
    de30 AS (
        SELECT client_id, COUNT(*)::int AS ev_30
        FROM dwh.digital_events
        WHERE event_date >= %(rd)s::date - INTERVAL '30 days'
          AND event_date <= %(rd)s::date
        GROUP BY client_id
    ),
    prod_cnt AS (
        SELECT client_id, COUNT(*)::int AS active_products
        FROM dwh.products
        WHERE status = 'ACTIVE'
        GROUP BY client_id
    ),
    metrics AS (
        SELECT
            c.client_id,
            COALESCE(c.registration_date, (c.created_at AT TIME ZONE 'UTC')::date) AS anchor_date,
            COALESCE(t.op_30, 0) AS op_30,
            COALESCE(t.debit_30, 0) + COALESCE(t.credit_30, 0) AS turnover_30,
            la.last_tx_ts,
            ld.last_ev_ts,
            COALESCE(d.ev_30, 0) AS ev_30,
            COALESCE(pc.active_products, 0) AS active_products
        FROM dwh.clients c
        LEFT JOIN tx30 t ON t.client_id = c.client_id
        LEFT JOIN last_any la ON la.client_id = c.client_id
        LEFT JOIN last_dig ld ON ld.client_id = c.client_id
        LEFT JOIN de30 d ON d.client_id = c.client_id
        LEFT JOIN prod_cnt pc ON pc.client_id = c.client_id
    ),
    classified AS (
        SELECT
            client_id,
            CASE
                WHEN anchor_date >= %(rd)s::date - INTERVAL '90 days' THEN 3::smallint
                WHEN anchor_date < %(rd)s::date - INTERVAL '90 days'
                    AND (
                        (last_tx_ts IS NULL AND last_ev_ts IS NULL)
                        OR GREATEST(
                            COALESCE((last_tx_ts AT TIME ZONE 'UTC')::date, DATE '1970-01-01'),
                            COALESCE((last_ev_ts AT TIME ZONE 'UTC')::date, DATE '1970-01-01')
                        ) < %(rd)s::date - INTERVAL '180 days'
                    )
                    THEN 4::smallint
                WHEN turnover_30 >= 250000 OR op_30 >= 12 OR active_products >= 4 THEN 5::smallint
                WHEN op_30 >= 3 OR ev_30 >= 6 OR turnover_30 >= 15000 THEN 1::smallint
                ELSE 2::smallint
            END AS segment_type_id
        FROM metrics
    ),
    mx AS (
        SELECT COALESCE(MAX(segment_history_id), 0) AS base_id
        FROM dwh.client_segments_history
    )
    INSERT INTO dwh.client_segments_history (
        segment_history_id, client_id, segment_type_id, score, effective_from, effective_to,
        is_current, assigned_by
    )
    SELECT
        mx.base_id + ROW_NUMBER() OVER (ORDER BY classified.client_id),
        classified.client_id,
        classified.segment_type_id,
        NULL::numeric,
        %(rd)s::date,
        NULL::date,
        true,
        'ETL_SEGMENTATION'
    FROM classified
    CROSS JOIN mx;
    """
    with conn.cursor() as cur:
        cur.execute(delete_sql)
        cur.execute(insert_sql, {"rd": report_date})


def refresh_profile_export(conn: psycopg.Connection, report_date: date | None = None) -> None:
    report_date = report_date or date.today()
    insert_sql = """
    INSERT INTO dwh.client_profile_export (
        report_date, client_id, unified_client_key, client_type, status, region_code,
        active_accounts_cnt, active_products_cnt,
        debit_turnover_30d, credit_turnover_30d,
        operations_cnt_30d, digital_events_cnt_30d, appeals_cnt_90d,
        last_transaction_ts, last_digital_event_ts, current_segment_type_id
    )
    SELECT
        %(d)s::date,
        c.client_id,
        c.unified_client_key,
        c.client_type,
        c.status,
        c.region_code,
        COALESCE(ac.cnt, 0),
        COALESCE(pc.cnt, 0),
        COALESCE(t.debit_amt, 0),
        COALESCE(t.credit_amt, 0),
        COALESCE(t.op_cnt, 0),
        COALESCE(e.ev_cnt, 0),
        COALESCE(a.ap_cnt, 0),
        t.last_ts,
        e.last_ev_ts,
        s.segment_type_id
    FROM dwh.clients c
    LEFT JOIN LATERAL (
        SELECT COUNT(*)::int AS cnt FROM dwh.accounts a
        WHERE a.client_id = c.client_id AND a.status = 'ACTIVE'
    ) ac ON true
    LEFT JOIN LATERAL (
        SELECT COUNT(*)::int AS cnt FROM dwh.products p
        WHERE p.client_id = c.client_id AND p.status = 'ACTIVE'
    ) pc ON true
    LEFT JOIN LATERAL (
        SELECT
            COALESCE(SUM(CASE WHEN x.direction = 'D' THEN x.amount ELSE 0 END), 0) AS debit_amt,
            COALESCE(SUM(CASE WHEN x.direction = 'C' THEN x.amount ELSE 0 END), 0) AS credit_amt,
            COUNT(*)::int AS op_cnt,
            MAX(x.operation_ts) AS last_ts
        FROM dwh.transactions x
        WHERE x.client_id = c.client_id
          AND x.operation_date >= %(d)s::date - INTERVAL '30 days'
    ) t ON true
    LEFT JOIN LATERAL (
        SELECT COUNT(*)::int AS ev_cnt, MAX(de.event_ts) AS last_ev_ts
        FROM dwh.digital_events de
        WHERE de.client_id = c.client_id
          AND de.event_date >= %(d)s::date - INTERVAL '30 days'
    ) e ON true
    LEFT JOIN LATERAL (
        SELECT COUNT(*)::int AS ap_cnt
        FROM dwh.appeals ap
        WHERE ap.client_id = c.client_id
          AND ap.created_ts::date >= %(d)s::date - INTERVAL '90 days'
    ) a ON true
    LEFT JOIN LATERAL (
        SELECT h.segment_type_id
        FROM dwh.client_segments_history h
        WHERE h.client_id = c.client_id AND h.is_current
        ORDER BY h.effective_from DESC
        LIMIT 1
    ) s ON true
    """
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM dwh.client_profile_export WHERE report_date = %s",
            (report_date,),
        )
        cur.execute(insert_sql, {"d": report_date})

def truncate_dwh_and_stg(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                dwh.client_profile_export,
                dwh.client_segments_history,
                dwh.appeals,
                dwh.digital_events,
                dwh.transactions,
                dwh.products,
                dwh.accounts,
                dwh.client_history,
                dwh.client_source_map,
                dwh.clients
            RESTART IDENTITY CASCADE
            """
        )
        cur.execute(
            """
            TRUNCATE TABLE
                stg.appeals_raw,
                stg.dbo_events_raw,
                stg.abs_transactions_raw,
                stg.abs_products_raw,
                stg.abs_accounts_raw,
                stg.crm_clients_raw,
                stg.abs_clients_raw,
                stg.load_batch
            RESTART IDENTITY CASCADE
            """
        )


def run_load_sources_to_staging(context: dict[str, Any]) -> int:
    conf = dag_conf(context)
    reset = bool(conf.get("reset", False))
    sample_dir = sample_data_dir(_DAGS_DIR)
    ds_dir = datasets_data_dir(_DAGS_DIR)
    roots = [sample_dir]
    if ds_dir.is_dir():
        roots.append(ds_dir)
    url = database_url()
    with psycopg.connect(url, autocommit=False) as conn:
        if reset:
            truncate_dwh_and_stg(conn)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.load_batch (source_system_code, status, notes)
                VALUES ('SAMPLE_CSV', 'RUNNING', 'CSV: data/sample + data/datasets')
                RETURNING batch_id
                """
            )
            batch_id = int(cur.fetchone()[0])

        load_all_csv(conn, batch_id, roots, manifest_dir=ds_dir if ds_dir.is_dir() else None)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stg.load_batch SET status = %s, finished_at = now() WHERE batch_id = %s",
                ("LOADED", batch_id),
            )
        conn.commit()
    return batch_id


def run_transform_stg_to_dwh(context: dict[str, Any], batch_id: int) -> int:
    url = database_url()
    with psycopg.connect(url, autocommit=False) as conn:
        transform_batch(conn, batch_id)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stg.load_batch SET status = %s, notes = notes || ' -> DWH' WHERE batch_id = %s",
                ("DONE", batch_id),
            )
        conn.commit()
    return batch_id


def run_refresh_client_profile_export(context: dict[str, Any]) -> None:
    conf = dag_conf(context)
    report_dt = report_date_for_dag(context, conf)
    url = database_url()
    with psycopg.connect(url, autocommit=False) as conn:
        recompute_client_segments(conn, report_dt)
        refresh_profile_export(conn, report_date=report_dt)
        conn.commit()


@dag(
    dag_id="load_data_to_dwh",
    description="Загрузка из источников (CSV) в stg, преобразование в dwh",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dwh", "etl", "postgresql"],
    doc_md=__doc__,
    default_args={
        "owner": "dwh",
        "retries": 1,
    },
)
def load_data_to_dwh():
    @task(task_id="load_sources_to_staging")
    def load_sources_to_staging() -> int:
        return run_load_sources_to_staging(get_current_context())

    @task(task_id="transform_stg_to_dwh")
    def transform_stg_to_dwh(batch_id: int) -> int:
        return run_transform_stg_to_dwh(get_current_context(), batch_id)

    @task(task_id="refresh_client_profile_export")
    def refresh_client_profile_export() -> None:
        run_refresh_client_profile_export(get_current_context())

    b = load_sources_to_staging()
    t = transform_stg_to_dwh(b)
    r = refresh_client_profile_export()
    t >> r


load_data_to_dwh()
