"""Переменные окружения и пути к данным."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "dwh")
    password = os.getenv("POSTGRES_PASSWORD", "dwh_secret")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "bank_dwh")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def sample_data_dir(dags_dir: Path) -> Path:
    """dags_dir — каталог airflow/dags (Path(__file__).resolve().parent в DAG-файле)."""
    env = os.getenv("ETL_SAMPLE_DATA_DIR")
    if env:
        return Path(env)
    airflow_home = dags_dir.resolve().parent
    if airflow_home.resolve() == Path("/opt/airflow"):
        return Path("/opt/airflow/data/sample")
    return airflow_home.parent / "data" / "sample"
