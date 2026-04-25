#!/usr/bin/env bash
# Выполняется образом postgres из /docker-entrypoint-initdb.d
(
  set -euo pipefail
  ROOT=/docker-entrypoint-initdb.d

  psql_q() {
    psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f "$1"
  }

  psql_q "${ROOT}/dwh/schema/dwh.sql"

  for t in \
    source_systems channels product_types operation_types event_types appeal_types segment_types \
    clients client_source_map client_history accounts products \
    ref_product_catalog crm_marketing_leads credit_risk_features \
    transactions digital_events appeals \
    client_segments_history client_profile_export
  do
    psql_q "${ROOT}/dwh/table/${t}.sql"
  done

  for idx in \
    idx_client_source_map_client idx_client_history_client idx_accounts_client idx_products_client \
    idx_transactions_client_date idx_transactions_op_ts idx_digital_events_client_date \
    idx_appeals_client idx_segments_client_current idx_profile_export_report
  do
    psql_q "${ROOT}/dwh/index/${idx}.sql"
  done

  psql_q "${ROOT}/stg/schema/stg.sql"

  for t in \
    load_batch abs_clients_raw crm_clients_raw abs_accounts_raw abs_products_raw product_catalog_raw \
    abs_transactions_raw dbo_events_raw appeals_raw crm_marketing_raw credit_risk_raw
  do
    psql_q "${ROOT}/stg/table/${t}.sql"
  done

  for idx in \
    idx_stg_abs_clients_batch idx_stg_crm_clients_batch idx_stg_abs_accounts_batch \
    idx_stg_abs_products_batch idx_stg_product_catalog_batch idx_stg_abs_tx_batch \
    idx_stg_dbo_events_batch idx_stg_appeals_batch idx_stg_crm_marketing_batch idx_stg_credit_risk_batch
  do
    psql_q "${ROOT}/stg/index/${idx}.sql"
  done

  for t in source_systems channels product_types operation_types event_types appeal_types segment_types
  do
    psql_q "${ROOT}/dwh/seed/${t}.sql"
  done

  psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d postgres -f "${ROOT}/airflow/database/airflow.sql"
)
