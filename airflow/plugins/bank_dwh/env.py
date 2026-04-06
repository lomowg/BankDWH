"""Переменные окружения и пути к данным."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _with_pg_utf8_client_encoding(url: str) -> str:
    lower = url.lower()
    if "client_encoding=" in lower:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}client_encoding=utf8"


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return _with_pg_utf8_client_encoding(url)
    user = os.getenv("POSTGRES_USER", "dwh")
    password = os.getenv("POSTGRES_PASSWORD", "dwh_secret")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "bank_dwh")
    return _with_pg_utf8_client_encoding(
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )


def sample_data_dir(dags_dir: Path) -> Path:
    """dags_dir — каталог airflow/dags (Path(__file__).resolve().parent в DAG-файле)."""
    env = os.getenv("ETL_SAMPLE_DATA_DIR")
    if env:
        return Path(env)
    airflow_home = dags_dir.resolve().parent
    if airflow_home.resolve() == Path("/opt/airflow"):
        return Path("/opt/airflow/data/sample")
    return airflow_home.parent / "data" / "sample"


def datasets_data_dir(dags_dir: Path) -> Path:
    """Дополнительные CSV в ./datasets"""
    env = os.getenv("ETL_DATASETS_DIR")
    if env:
        return Path(env)
    airflow_home = dags_dir.resolve().parent
    if airflow_home.resolve() == Path("/opt/airflow"):
        return Path("/opt/airflow/data/datasets")
    return airflow_home.parent / "data" / "datasets"
