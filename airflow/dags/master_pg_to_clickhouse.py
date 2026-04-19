from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState

default_args = {
    "owner": "dwh",
    "retries": 1,
}

with DAG(
    dag_id="master_pg_to_clickhouse",
    description="PostgreSQL → ClickHouse (витрины)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dwh", "postgresql", "clickhouse", "orchestration"],
    doc_md=__doc__,
    default_args=default_args,
) as dag:
    run_load_data_to_dwh = TriggerDagRunOperator(
        task_id="run_load_data_to_dwh",
        trigger_dag_id="load_data_to_dwh",
        logical_date="{{ logical_date }}",
        conf="{{ dag_run.conf }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_load_marts_to_ch = TriggerDagRunOperator(
        task_id="run_load_marts_to_ch",
        trigger_dag_id="load_marts_to_ch",
        logical_date="{{ logical_date }}",
        conf="{{ dag_run.conf }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_load_data_to_dwh >> run_load_marts_to_ch
