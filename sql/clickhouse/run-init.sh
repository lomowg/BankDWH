#!/usr/bin/env bash
# Выполняется образом ClickHouse из /docker-entrypoint-initdb.d (подкаталоги не обрабатываются автоматически).
(
  set -euo pipefail
  ROOT=/docker-entrypoint-initdb.d

  clickhouse-client -n --queries-file "${ROOT}/database/bank_marts.sql"

  shopt -s nullglob
  for f in "${ROOT}/bank_marts/table/"*.sql
  do
    clickhouse-client -n --queries-file "$f"
  done
  for f in "${ROOT}/bank_marts/dictionary/"*.sql
  do
    clickhouse-client -n --queries-file "$f"
  done
  for f in "${ROOT}/bank_marts/view/"*.sql
  do
    clickhouse-client -n --queries-file "$f"
  done
)
