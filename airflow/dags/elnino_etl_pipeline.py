"""
DAG — El Niño ETL Case Study (Open-Meteo -> MinIO -> DuckDB)

Pipeline (sesuai Langkah 6-9 kelas):
  check_connections
        >> ingest_open_meteo   (dynamic task mapping: dataset x lokasi)
        >> build_bronze        (JSON di MinIO -> bronze.open_meteo_daily)
        >> build_silver        (dim + fact: weather / drought / fire / mitigation)
        >> build_gold          (gold.* dashboard-ready)
        >> run_analytic_queries (validasi + contoh insight)

Berjalan di stack lokal peserta: Airflow + MinIO + DuckDB.
MinIO dibaca dari Airflow Variables: MINIO_ENDPOINT, MINIO_ACCESS_KEY,
MINIO_SECRET_KEY, MINIO_BUCKET.
"""

import io
import logging
from datetime import datetime, timedelta

import duckdb
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from elnino_common import (
    DATASETS,
    LOCATIONS,
    DUCKDB_PATH,
    SILVER_SQL,
    GOLD_SQL,
    fetch_open_meteo,
    get_minio,
    upload_json,
)

log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Task: check connections ───────────────────────────────────
def check_connections(**context):
    endpoint = Variable.get("MINIO_ENDPOINT")
    access = Variable.get("MINIO_ACCESS_KEY")
    secret = Variable.get("MINIO_SECRET_KEY")
    bucket = Variable.get("MINIO_BUCKET")

    client = get_minio(endpoint, access, secret)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Bucket '{bucket}' dibuat.")
    else:
        log.info(f"MinIO OK -> bucket '{bucket}' exists.")


# ── Task: ingest (dynamic mapping) ────────────────────────────
def ingest_one(dataset: str, location_key: str, **context):
    """
    Satu task per (dataset, lokasi). Fetch Open-Meteo lalu tulis JSON
    ke MinIO landing zone:
      raw/open-meteo/{dataset}/location={location}/ingestion_date={ds}/{ts}.json
    """
    endpoint = Variable.get("MINIO_ENDPOINT")
    access = Variable.get("MINIO_ACCESS_KEY")
    secret = Variable.get("MINIO_SECRET_KEY")
    bucket = Variable.get("MINIO_BUCKET")
    ds = context["ds"]

    location = next(loc for loc in LOCATIONS if loc[0] == location_key)
    records = fetch_open_meteo(location, dataset, ds)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    object_path = f"raw/open-meteo/{dataset}/location={location_key}/ingestion_date={ds}/{ts}.json"

    client = get_minio(endpoint, access, secret)
    upload_json(client, bucket, object_path, records)

    log.info(f"{dataset}/{location_key}: {len(records)} records -> {object_path}")
    return {"dataset": dataset, "location": location_key, "rows": len(records)}


# ── Task: build bronze ────────────────────────────────────────
def build_bronze(**context):
    endpoint = Variable.get("MINIO_ENDPOINT")
    access = Variable.get("MINIO_ACCESS_KEY")
    secret = Variable.get("MINIO_SECRET_KEY")
    bucket = Variable.get("MINIO_BUCKET")
    ds = context["ds"]

    client = get_minio(endpoint, access, secret)
    prefix = "raw/open-meteo/"
    all_records = []
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if f"ingestion_date={ds}/" not in obj.object_name:
            continue
        resp = client.get_object(bucket, obj.object_name)
        data = resp.read()
        resp.close()
        resp.release_conn()
        all_records.extend(json_loads(data))

    if not all_records:
        raise ValueError(f"Tidak ada data MinIO untuk ingestion_date={ds}")

    df = pd.DataFrame(all_records)
    # Dedupe overlap archive/forecast di tanggal run_date (prioritaskan archive)
    df = df.sort_values("dataset").drop_duplicates(subset=["date", "location"], keep="first")

    conn = duckdb.connect(DUCKDB_PATH)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        conn.execute("DROP TABLE IF EXISTS bronze.open_meteo_daily;")
        conn.register("src", df)
        conn.execute("CREATE TABLE bronze.open_meteo_daily AS SELECT * FROM src;")
        cnt = conn.execute("SELECT COUNT(*) FROM bronze.open_meteo_daily").fetchone()[0]
        log.info(f"bronze.open_meteo_daily: {cnt:,} rows")
    finally:
        conn.close()


def json_loads(data: bytes):
    import json
    return json.loads(data)


# ── Task: build silver / gold ─────────────────────────────────
def _run_sql(conn, sql: str):
    """Jalankan setiap statement yang dipisahkan oleh ';' (DuckDB execute = 1 statement)."""
    for stmt in sql.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)


def build_silver(**context):
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        for schema in ("silver", "gold"):
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        _run_sql(conn, SILVER_SQL)
        for tbl in ("dim_date", "dim_location", "fact_weather_daily",
                    "fact_drought_risk_daily", "fact_fire_risk_daily", "fact_mitigation_daily"):
            cnt = conn.execute(f"SELECT COUNT(*) FROM silver.{tbl}").fetchone()[0]
            log.info(f"silver.{tbl}: {cnt:,} rows")
    finally:
        conn.close()


def build_gold(**context):
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        _run_sql(conn, GOLD_SQL)
        for tbl in ("drought_risk_by_region", "el_nino_weather_trend",
                    "karhutla_risk_by_region", "mitigation_priority_daily"):
            cnt = conn.execute(f"SELECT COUNT(*) FROM gold.{tbl}").fetchone()[0]
            log.info(f"gold.{tbl}: {cnt:,} rows")
    finally:
        conn.close()


# ── Task: analytic queries (insight demo) ─────────────────────
def run_analytic_queries(**context):
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        log.info("=== Top 5 wilayah risiko kekeringan tertinggi ===")
        q = """
            SELECT l.province, l.location_name, MAX(f.drought_risk_score) AS peak
            FROM silver.fact_drought_risk_daily f
            JOIN silver.dim_location l ON f.location_key = l.location_key
            GROUP BY 1,2 ORDER BY peak DESC LIMIT 5;
        """
        for r in conn.execute(q).fetchall():
            log.info(f"  {r[0]} / {r[1]}: {r[2]:.1f}")

        log.info("=== Top 5 wilayah potensi karhutla tertinggi ===")
        q = """
            SELECT l.province, l.location_name, MAX(f.fire_risk_score) AS peak
            FROM silver.fact_fire_risk_daily f
            JOIN silver.dim_location l ON f.location_key = l.location_key
            GROUP BY 1,2 ORDER BY peak DESC LIMIT 5;
        """
        for r in conn.execute(q).fetchall():
            log.info(f"  {r[0]} / {r[1]}: {r[2]:.1f}")
    finally:
        conn.close()


# ── DAG definition ────────────────────────────────────────────
INGEST_JOBS = [{"dataset": d, "location_key": loc[0]} for d in DATASETS for loc in LOCATIONS]

with DAG(
    dag_id="elnino_etl_pipeline",
    default_args=default_args,
    description="El Niño ETL: Open-Meteo -> MinIO -> DuckDB (bronze/silver/gold)",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elnino", "open-meteo", "minio", "duckdb", "case-study"],
    doc_md="""
## El Niño ETL Pipeline

**Sumber:** Open-Meteo API (`/v1/forecast`, `/v1/archive`)
**Landing:** MinIO `raw/open-meteo/{dataset}/location={loc}/ingestion_date={ds}/*.json`
**Warehouse:** DuckDB (`/opt/airflow/dags/repo/elnino.duckdb`)

**Pipeline:**
```
check_connections
      >> ingest_open_meteo      (map: 2 dataset x 15 lokasi)
      >> build_bronze           (JSON -> bronze.open_meteo_daily)
      >> build_silver           (dim_date, dim_location, fact_*)
      >> build_gold             (gold.* dashboard-ready)
      >> run_analytic_queries   (contoh insight)
```

**Data model (star schema):**
- Dimensions: `silver.dim_date`, `silver.dim_location`
- Facts: `silver.fact_weather_daily` (+ crop_stress_score), `fact_drought_risk_daily`,
  `fact_fire_risk_daily`, `fact_mitigation_daily`
- Gold: `drought_risk_by_region`, `el_nino_weather_trend`, `karhutla_risk_by_region`,
  `mitigation_priority_daily`

Set Airflow Variables MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY / MINIO_BUCKET
sama seperti DAG AdventureWorks (Mis. `elt_minio:9000`).
    """,
) as dag:

    task_check = PythonOperator(task_id="check_connections", python_callable=check_connections)

    task_ingest = PythonOperator.partial(
        task_id="ingest_open_meteo",
        python_callable=ingest_one,
    ).expand(op_kwargs=INGEST_JOBS)

    task_bronze = PythonOperator(task_id="build_bronze", python_callable=build_bronze)
    task_silver = PythonOperator(task_id="build_silver", python_callable=build_silver)
    task_gold = PythonOperator(task_id="build_gold", python_callable=build_gold)
    task_analytics = PythonOperator(task_id="run_analytic_queries", python_callable=run_analytic_queries)

    task_check >> task_ingest >> task_bronze >> task_silver >> task_gold >> task_analytics
