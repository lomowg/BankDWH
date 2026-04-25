FROM apache/airflow:2.10.4-python3.12

USER airflow
RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.2" \
    "python-dotenv>=1.0" \
    "clickhouse-connect>=0.8.0" \
    "openpyxl>=3.1"
