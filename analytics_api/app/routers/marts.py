from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.clickhouse_db import fetch_all

router = APIRouter()

_MAX_LIMIT = 2000
_DEFAULT_LIMIT = 200
_MAX_RANGE_DAYS = 400


def _clamp_limit(limit: int) -> int:
    return max(1, min(limit, _MAX_LIMIT))


@router.get("/client-profile", summary="Витрина профиля клиента (mart_client_profile)")
def client_profile(
    report_date: date | None = Query(None, description="Отчётная дата; если не задана — max(report_date) в витрине"),
    client_id: int | None = Query(None, ge=1),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    lim = _clamp_limit(limit)
    if report_date is not None:
        params: dict[str, Any] = {"rd": report_date, "lim": lim, "off": offset}
        extra = " AND client_id = {cid:UInt64}" if client_id is not None else ""
        if client_id is not None:
            params["cid"] = client_id
        sql = f"""
            SELECT
                report_date, client_id, unified_client_key, client_type, status, region_code,
                active_accounts_cnt, active_products_cnt,
                debit_turnover_30d, credit_turnover_30d,
                operations_cnt_30d, digital_events_cnt_30d, appeals_cnt_90d,
                last_transaction_ts, last_digital_event_ts, current_segment_type_id, loaded_at
            FROM mart_client_profile
            WHERE report_date = {{rd:Date}}{extra}
            ORDER BY client_id
            LIMIT {{lim:UInt32}} OFFSET {{off:UInt32}}
        """
        return fetch_all(sql, params)
    extra2 = " AND client_id = {cid:UInt64}" if client_id is not None else ""
    params2: dict[str, Any] = {"lim": lim, "off": offset}
    if client_id is not None:
        params2["cid"] = client_id
    sql = f"""
        SELECT
            report_date, client_id, unified_client_key, client_type, status, region_code,
            active_accounts_cnt, active_products_cnt,
            debit_turnover_30d, credit_turnover_30d,
            operations_cnt_30d, digital_events_cnt_30d, appeals_cnt_90d,
            last_transaction_ts, last_digital_event_ts, current_segment_type_id, loaded_at
        FROM mart_client_profile
        WHERE report_date = (SELECT max(report_date) FROM mart_client_profile)
        {extra2}
        ORDER BY client_id
        LIMIT {{lim:UInt32}} OFFSET {{off:UInt32}}
    """
    return fetch_all(sql, params2)


@router.get("/client-profile/{client_id}", summary="Профиль одного клиента (последняя отчётная дата)")
def client_profile_one(
    client_id: int,
    report_date: date | None = Query(None),
) -> dict[str, Any]:
    if report_date is not None:
        rows = fetch_all(
            """
            SELECT
                report_date, client_id, unified_client_key, client_type, status, region_code,
                active_accounts_cnt, active_products_cnt,
                debit_turnover_30d, credit_turnover_30d,
                operations_cnt_30d, digital_events_cnt_30d, appeals_cnt_90d,
                last_transaction_ts, last_digital_event_ts, current_segment_type_id, loaded_at
            FROM mart_client_profile
            WHERE client_id = {cid:UInt64} AND report_date = {rd:Date}
            LIMIT 1
            """,
            {"cid": client_id, "rd": report_date},
        )
    else:
        rows = fetch_all(
            """
            SELECT
                report_date, client_id, unified_client_key, client_type, status, region_code,
                active_accounts_cnt, active_products_cnt,
                debit_turnover_30d, credit_turnover_30d,
                operations_cnt_30d, digital_events_cnt_30d, appeals_cnt_90d,
                last_transaction_ts, last_digital_event_ts, current_segment_type_id, loaded_at
            FROM mart_client_profile
            WHERE client_id = {cid:UInt64}
            ORDER BY report_date DESC
            LIMIT 1
            """,
            {"cid": client_id},
        )
    if not rows:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return rows[0]


@router.get("/segment-metrics", summary="Метрики по сегментам (mart_segment_metrics)")
def segment_metrics(
    report_date: date | None = Query(None),
) -> list[dict[str, Any]]:
    if report_date is not None:
        return fetch_all(
            """
            SELECT
                report_date, segment_type_id, clients_cnt, active_clients_30d,
                total_debit_turnover_30d, total_credit_turnover_30d,
                total_operations_30d, avg_operations_per_client, loaded_at
            FROM mart_segment_metrics
            WHERE report_date = {rd:Date}
            ORDER BY segment_type_id
            """,
            {"rd": report_date},
        )
    return fetch_all(
        """
        SELECT
            report_date, segment_type_id, clients_cnt, active_clients_30d,
            total_debit_turnover_30d, total_credit_turnover_30d,
            total_operations_30d, avg_operations_per_client, loaded_at
        FROM mart_segment_metrics
        WHERE report_date = (SELECT max(report_date) FROM mart_segment_metrics)
        ORDER BY segment_type_id
        """
    )


@router.get("/client-activity-daily", summary="Активность по дням и каналам (mart_client_activity_daily)")
def client_activity_daily(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    client_id: int | None = Query(None, ge=1),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    if (date_to - date_from).days > _MAX_RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"Интервал не более {_MAX_RANGE_DAYS} дней")
    lim = _clamp_limit(limit)
    params: dict[str, Any] = {"d0": date_from, "d1": date_to, "lim": lim, "off": offset}
    extra = " AND client_id = {cid:UInt64}" if client_id is not None else ""
    if client_id is not None:
        params["cid"] = client_id
    sql = f"""
        SELECT
            activity_date, client_id, channel_code, operations_cnt,
            debit_amount, credit_amount, digital_events_cnt,
            digital_success_cnt, digital_fail_cnt, appeals_opened_cnt, loaded_at
        FROM mart_client_activity_daily
        WHERE activity_date >= {{d0:Date}} AND activity_date <= {{d1:Date}}{extra}
        ORDER BY activity_date, client_id, channel_code
        LIMIT {{lim:UInt32}} OFFSET {{off:UInt32}}
    """
    return fetch_all(sql, params)


@router.get("/financial-activity-daily", summary="Финансовая активность по типу операции (mart_financial_activity_daily)")
def financial_activity_daily(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    client_id: int | None = Query(None, ge=1),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    if (date_to - date_from).days > _MAX_RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"Интервал не более {_MAX_RANGE_DAYS} дней")
    lim = _clamp_limit(limit)
    params: dict[str, Any] = {"d0": date_from, "d1": date_to, "lim": lim, "off": offset}
    extra = " AND client_id = {cid:UInt64}" if client_id is not None else ""
    if client_id is not None:
        params["cid"] = client_id
    sql = f"""
        SELECT
            activity_date, client_id, channel_code, operation_type_code,
            operation_cnt, debit_amount, credit_amount, loaded_at
        FROM mart_financial_activity_daily
        WHERE activity_date >= {{d0:Date}} AND activity_date <= {{d1:Date}}{extra}
        ORDER BY activity_date, client_id, channel_code, operation_type_code
        LIMIT {{lim:UInt32}} OFFSET {{off:UInt32}}
    """
    return fetch_all(sql, params)


@router.get("/digital-activity-daily", summary="Цифровая активность по типу события (mart_digital_activity_daily)")
def digital_activity_daily(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    client_id: int | None = Query(None, ge=1),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    if (date_to - date_from).days > _MAX_RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"Интервал не более {_MAX_RANGE_DAYS} дней")
    lim = _clamp_limit(limit)
    params: dict[str, Any] = {"d0": date_from, "d1": date_to, "lim": lim, "off": offset}
    extra = " AND client_id = {cid:UInt64}" if client_id is not None else ""
    if client_id is not None:
        params["cid"] = client_id
    sql = f"""
        SELECT
            activity_date, client_id, channel_code, event_type_code,
            event_cnt, success_cnt, fail_cnt, loaded_at
        FROM mart_digital_activity_daily
        WHERE activity_date >= {{d0:Date}} AND activity_date <= {{d1:Date}}{extra}
        ORDER BY activity_date, client_id, channel_code, event_type_code
        LIMIT {{lim:UInt32}} OFFSET {{off:UInt32}}
    """
    return fetch_all(sql, params)
