"""Конфиг DAG run и даты из контекста Airflow."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


def dag_conf(context: dict[str, Any]) -> dict[str, Any]:
    dr = context.get("dag_run")
    if dr and dr.conf:
        return dict(dr.conf)
    return {}


def report_date_for_dag(context: dict[str, Any], conf: dict[str, Any] | None = None) -> date:
    if conf is None:
        conf = dag_conf(context)
    if conf.get("report_date"):
        return date.fromisoformat(str(conf["report_date"]))
    logical = context.get("logical_date") or context.get("data_interval_end")
    if logical is not None:
        if hasattr(logical, "date"):
            return logical.date()
        if isinstance(logical, datetime):
            return logical.date()
    return date.today()


def activity_date_range(context: dict[str, Any], conf: dict[str, Any] | None = None) -> tuple[date, date]:
    if conf is None:
        conf = dag_conf(context)
    if conf.get("activity_from") and conf.get("activity_to"):
        return (
            date.fromisoformat(str(conf["activity_from"])),
            date.fromisoformat(str(conf["activity_to"])),
        )
    end = report_date_for_dag(context, conf)
    days = int(conf.get("activity_days_back", 400))
    return end - timedelta(days=days), end
