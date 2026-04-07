# Run from repo root: powershell -ExecutionPolicy Bypass -File k8s/apply.ps1
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$ns = "bank-dwh"

kubectl cluster-info --request-timeout=15s *> $null
if (-not $?) {
    Write-Host ""
    Write-Host "ERROR: Kubernetes API is not reachable (cluster stopped or wrong kubectl context)."
    Write-Host "Fix:"
    Write-Host "  - kind:  docker start (if nodes stopped)  OR  kind create cluster"
    Write-Host "  - check: kubectl config current-context"
    Write-Host "  - test:  kubectl cluster-info"
    Write-Host ""
    exit 1
}

kubectl apply --validate=false -f "$PSScriptRoot/manifests/namespace.yaml"
kubectl apply --validate=false -f "$PSScriptRoot/manifests/secrets.yaml"

function New-FlatSqlConfigMap {
    param([string]$Name, [string]$Dir)
    $rootDir = Join-Path $Root $Dir
    $fromArgs = @()
    Get-ChildItem -Path $rootDir -File -Recurse | ForEach-Object {
        $rel = $_.FullName.Substring($rootDir.Length + 1).Replace('\', '/')
        $key = $rel.Replace('/', '__')
        $fromArgs += "--from-file=$key=$($_.FullName)"
    }
    & kubectl create configmap $Name -n $ns @fromArgs --dry-run=client -o yaml | kubectl apply --validate=false -f -
}

New-FlatSqlConfigMap postgres-init-sql sql/postgresql
New-FlatSqlConfigMap clickhouse-init-sql sql/clickhouse
kubectl create configmap prometheus-config -n $ns --from-file="prometheus.yml=monitoring/prometheus/prometheus.yml" --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap blackbox-config -n $ns --from-file="blackbox.yml=monitoring/blackbox/blackbox.yml" --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap grafana-datasources -n $ns --from-file="monitoring/grafana/provisioning/datasources/" --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap grafana-dashboards-provisioning -n $ns --from-file="monitoring/grafana/provisioning/dashboards/" --dry-run=client -o yaml | kubectl apply --validate=false -f -
kubectl create configmap grafana-dashboard-json -n $ns --from-file="monitoring/grafana/dashboards/" --dry-run=client -o yaml | kubectl apply --validate=false -f -

kubectl apply --validate=false -f "$PSScriptRoot/manifests/postgres.yaml"
kubectl apply --validate=false -f "$PSScriptRoot/manifests/clickhouse.yaml"
kubectl apply --validate=false -f "$PSScriptRoot/manifests/monitoring.yaml"
kubectl apply --validate=false -f "$PSScriptRoot/manifests/airflow.yaml"
kubectl apply --validate=false -f "$PSScriptRoot/manifests/analytics-api.yaml"

Write-Host "Done. Build images from repo root:"
Write-Host "  docker build -f Dockerfile.airflow.k8s -t bank_dwh_airflow:2.10.4-python3.12 ."
Write-Host "  docker build -t bank_dwh_analytics_api:1.0 ./analytics_api"
Write-Host "Load images into the cluster, then: kubectl get pods -n $ns"
