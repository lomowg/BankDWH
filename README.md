# BankDWH

## Запуск в Kubernetes (kind + Docker Desktop)

**Нужно заранее:** Docker Desktop запущен, установлены [kind](https://kind.sigs.k8s.io/) и `kubectl`, контекст указывает на ваш кластер (`kubectl cluster-info` без ошибки). Если кластера ещё нет:

```text
kind create cluster
```

**Перед первым деплоем** при необходимости отредактируйте пароли и строки подключения в `k8s/manifests/secrets.yaml`.

Дальше из **корня репозитория**:

```powershell
docker build -f Dockerfile.airflow.k8s -t bank_dwh_airflow:2.10.4-python3.12 .
docker build -t bank_dwh_analytics_api:1.0 ./analytics_api
kind load docker-image bank_dwh_airflow:2.10.4-python3.12
kind load docker-image bank_dwh_analytics_api:1.0
powershell -ExecutionPolicy Bypass -File k8s/apply.ps1
```

Если кластер kind не с именем по умолчанию, добавьте `--name <имя>` к командам `kind load`.

Проверка подов (ожидайте `Running` / готовность `1/1`):

```powershell
kubectl get pods -n bank-dwh
```

**Доступ к сервисам** (отдельное окно PowerShell на каждую команду):

```powershell
kubectl port-forward -n bank-dwh svc/airflow-webserver 8080:8080
kubectl port-forward -n bank-dwh svc/grafana 3000:3000
kubectl port-forward -n bank-dwh svc/prometheus 9090:9090
kubectl port-forward -n bank-dwh svc/analytics-api 8000:8000
```

**В браузере**

- Airflow: http://localhost:8080 — пользователь `airflow`, пароль: ключ `airflow-www-password` в `k8s/manifests/secrets.yaml`.
- Grafana: http://localhost:3000 — пользователь `admin`, пароль: ключ `grafana-admin-password` в том же файле.
- Analytics API: http://localhost:8000/docs

