#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NS=bank-dwh

if ! kubectl cluster-info --request-timeout=15s >/dev/null 2>&1; then
  echo ""
  echo "ERROR: Kubernetes API is not reachable (cluster stopped or wrong kubectl context)."
  echo "Fix:"
  echo "  - kind:  docker start ...  OR  kind create cluster"
  echo "  - check: kubectl config current-context"
  echo "  - test:  kubectl cluster-info"
  echo ""
  exit 1
fi

kubectl apply --validate=false -f k8s/manifests/namespace.yaml
kubectl apply --validate=false -f k8s/manifests/secrets.yaml


create_flat_sql_configmap() {
  local name="$1"
  local dir="$2"
  local args=()
  while IFS= read -r -d '' line; do
    args+=( "$line" )
  done < <(
    cd "$ROOT/$dir" || exit 1
    while IFS= read -r -d '' f; do
      rel="${f#./}"
      key="${rel//\//__}"
      printf '%s\0' "--from-file=${key}=${PWD}/${rel}"
    done < <(find . -type f -print0)
  )
  kubectl create configmap "$name" -n "$NS" "${args[@]}" --dry-run=client -o yaml | kubectl apply --validate=false -f -
}

create_flat_sql_configmap postgres-init-sql sql/postgresql
create_flat_sql_configmap clickhouse-init-sql sql/clickhouse
kubectl create configmap prometheus-config -n "$NS" --from-file=prometheus.yml=monitoring/prometheus/prometheus.yml --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap blackbox-config -n "$NS" --from-file=blackbox.yml=monitoring/blackbox/blackbox.yml --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap grafana-datasources -n "$NS" --from-file=monitoring/grafana/provisioning/datasources/ --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap grafana-dashboards-provisioning -n "$NS" --from-file=monitoring/grafana/provisioning/dashboards/ --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap grafana-dashboard-json -n "$NS" --from-file=monitoring/grafana/dashboards/ --dry-run=client -o yaml | kubectl apply --validate=false -f -

kubectl apply --validate=false -f k8s/manifests/postgres.yaml
kubectl apply --validate=false -f k8s/manifests/clickhouse.yaml
kubectl apply --validate=false -f k8s/manifests/monitoring.yaml
kubectl apply --validate=false -f k8s/manifests/airflow.yaml
kubectl apply --validate=false -f k8s/manifests/analytics-api.yaml

echo "Done. Build images from repo root:"
echo "  docker build -f Dockerfile.airflow.k8s -t bank_dwh_airflow:2.10.4-python3.12 ."
echo "  docker build -t bank_dwh_analytics_api:1.0 ./analytics_api"
echo "Load images into the cluster, then: kubectl get pods -n $NS"
