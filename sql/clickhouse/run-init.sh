#!/usr/bin/env bash
# Выполняется образом ClickHouse из /docker-entrypoint-initdb.d.
(
  set -euo pipefail
  ROOT=/docker-entrypoint-initdb.d

  clickhouse-client -n --queries-file "${ROOT}/database/bank_marts.sql"

  shopt -s nullglob
  for f in "${ROOT}/bank_marts/table/"*.sql
  do
    clickhouse-client -n --queries-file "$f"
  done
  sql_escape_quote() {
    printf '%s' "$1" | sed "s/'/''/g"
  }

  passwd_line=
  if [ -z "${CLICKHOUSE_PASSWORD:-}" ]; then
    passwd_line="password ''"
  else
    escaped=$(sql_escape_quote "$CLICKHOUSE_PASSWORD")
    passwd_line="password '${escaped}'"
  fi

  for f in "${ROOT}/bank_marts/dictionary/"*.sql
  do
    tmp=$(mktemp)
    sed "s|__CLICKHOUSE_PASSWORD_FOR_DICT__|${passwd_line}|g" "$f" >"$tmp"
    clickhouse-client -n --queries-file "$tmp"
    rm -f "$tmp"
  done
  for f in "${ROOT}/bank_marts/view/"*.sql
  do
    clickhouse-client -n --queries-file "$f"
  done
)
