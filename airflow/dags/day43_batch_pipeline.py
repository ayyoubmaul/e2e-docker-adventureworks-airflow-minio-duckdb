"""
dags/day43_batch_pipeline.py
============================
AdventureWorks Full Batch Pipeline — DAG E2E (Hadoop layer)

Architecture (Tahap 1 → 6)
--------------------------
PostgreSQL (AdventureWorks Source)
    ↓  [extract_to_hdfs]        jobs/hive_to_parquet.py --step extract
📂 HDFS Parquet Raw Layer
    ↓  [register_hive_tables]   jobs/hive_to_parquet.py --step register
🔷 Hive External Tables (adventureworks.*)
    ↓  [verify_raw_tables]      jobs/hive_to_parquet.py --step verify
    ↓
    ├─ [transform_sales]    →   sales_pipeline.py
    │                           adventureworks_curated.fact_sales_performance
    │                           adventureworks_curated.monthly_sales_summary
    │                           adventureworks_curated.territory_yoy
    │                           adventureworks_curated.top_products
    ├─ [transform_hr]       →   hr_pipeline.py
    │                           adventureworks_curated.fact_hr_workforce
    ├─ [transform_vendor]   →   vendor_pipeline.py
    │                           adventureworks_curated.fact_vendor_performance
    │                           adventureworks_curated.vendor_overall_ranking
    └─ [transform_rfm]      →   rfm_pipeline.py
                                adventureworks_curated.fact_customer_rfm
         ↓ (fan-in: all 4 parallel tasks must succeed)
    [validate_curated]      — row-count check, all 8 curated tables
         ↓
    [notify_complete]       — log pipeline summary

Schedule  : manual trigger (hands-on) — ubah `schedule` bila perlu
Retries   : 2 per task, 5-minute delay
Catchup   : False
"""

from datetime import datetime, timedelta

from airflow import DAG

from dag_utils import (
    make_spark_task,
    make_validation_task,
    make_notify_task,
)

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": False,
    "email_on_failure":  False,
    "email_on_retry":    False,
}

# ─── 8 tabel curated yang divalidasi setelah semua transform ──────────────────

CURATED_TABLES = [
    # sales_pipeline.py
    "adventureworks_curated.fact_sales_performance",
    "adventureworks_curated.monthly_sales_summary",
    "adventureworks_curated.territory_yoy",
    "adventureworks_curated.top_products",
    # hr_pipeline.py
    "adventureworks_curated.fact_hr_workforce",
    # vendor_pipeline.py
    "adventureworks_curated.fact_vendor_performance",
    "adventureworks_curated.vendor_overall_ranking",
    # rfm_pipeline.py
    "adventureworks_curated.fact_customer_rfm",
]

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="day43_adventureworks_batch_pipeline",
    default_args=default_args,
    description=(
        "AdventureWorks full batch pipeline: "
        "PostgreSQL → HDFS → Hive → Curated Layer (Sales, HR, Vendor, RFM)"
    ),
    start_date=datetime(2025, 1, 1),
    schedule=None,          # trigger manual via UI / `airflow dags trigger`
    catchup=False,
    max_active_runs=1,
    tags=["day43", "batch", "adventureworks", "pyspark"],
    doc_md=__doc__,
) as dag:

    # ── Tahap 1. Extract: PostgreSQL → HDFS Parquet ───────────────────────────
    extract_to_hdfs = make_spark_task(
        task_id="extract_to_hdfs",
        job_file="hive_to_parquet.py",
        extra_args="--step extract",
        driver_memory="1g",
        timeout_minutes=30,
    )

    # ── Tahap 2. Register: HDFS Parquet → Hive External Tables ────────────────
    register_hive_tables = make_spark_task(
        task_id="register_hive_tables",
        job_file="hive_to_parquet.py",
        extra_args="--step register",
        driver_memory="1g",
        timeout_minutes=10,
    )

    # ── Tahap 3. Verify: row-count check on raw Hive tables ───────────────────
    verify_raw_tables = make_spark_task(
        task_id="verify_raw_tables",
        job_file="hive_to_parquet.py",
        extra_args="--step verify",
        driver_memory="512m",
        timeout_minutes=5,
    )

    # ── Tahap 4. Transform — Fan-Out (4 task paralel) ─────────────────────────
    transform_sales = make_spark_task(
        task_id="transform_sales",
        job_file="sales_pipeline.py",
        driver_memory="1g",
        timeout_minutes=20,
    )

    transform_hr = make_spark_task(
        task_id="transform_hr",
        job_file="hr_pipeline.py",
        driver_memory="1g",
        timeout_minutes=20,
    )

    transform_vendor = make_spark_task(
        task_id="transform_vendor",
        job_file="vendor_pipeline.py",
        driver_memory="1g",
        timeout_minutes=20,
    )

    transform_rfm = make_spark_task(
        task_id="transform_rfm",
        job_file="rfm_pipeline.py",
        driver_memory="1g",
        timeout_minutes=20,
    )

    # ── Tahap 5. Validate — Fan-In: row-count check 8 curated tables ──────────
    validate_curated = make_validation_task(
        task_id="validate_curated",
        database="adventureworks_curated",
        tables=CURATED_TABLES,
        timeout_minutes=5,
    )

    # ── Tahap 6. Notify: log completion summary ───────────────────────────────
    notify_complete = make_notify_task(
        task_id="notify_complete",
        message="All 8 curated tables ready in adventureworks_curated.*",
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    #
    #  extract_to_hdfs
    #       ↓
    #  register_hive_tables
    #       ↓
    #  verify_raw_tables
    #       ↓  (fan-out: parallel)
    #  ┌────────┬────────┬────────┬─────┐
    #  │ sales  │   hr   │ vendor │ rfm │
    #  └────────┴────────┴────────┴─────┘
    #       ↓ (fan-in: all 4 must succeed)
    #  validate_curated
    #       ↓
    #  notify_complete

    transform_tasks = [transform_sales, transform_hr, transform_vendor, transform_rfm]

    (
        extract_to_hdfs
        >> register_hive_tables
        >> verify_raw_tables
        >> transform_tasks
        >> validate_curated
        >> notify_complete
    )
