"""
dags/dag_utils.py
=================
Shared helpers untuk DAG day43_batch_pipeline.

Pipeline Spark TIDAK jalan di dalam container Airflow — task Airflow
mengeksekusi job via `docker exec` ke container `day43-spark`
(docker compose -f docker-compose.spark.yml). Docker socket sudah
di-mount di airflow/docker-compose.yaml.

Provides:
  - make_spark_task()      : task spark-submit job di day43-spark
  - make_validation_task() : row-count check tabel curated
  - make_notify_task()     : log ringkasan pipeline selesai
"""

import os
from datetime import timedelta

from airflow.operators.python import PythonOperator

# ─── Config ───────────────────────────────────────────────────────────────────

SPARK_CONTAINER = os.environ.get("SPARK_CONTAINER", "day43-spark")
CONFIG          = "/app/configs/pipeline.yaml"
JARS            = "/app/jars/postgresql-42.7.3.jar"


def _exec_in_spark_container(cmd: str, task_id: str):
    """
    Jalankan command di dalam container spark via docker SDK (exec_run).
    Package `docker` ada di airflow/requirements.txt (perlu rebuild image).
    """
    try:
        import docker  # pip package `docker` — tambahkan di requirements.txt
    except ImportError:
        raise RuntimeError(
            "Package 'docker' tidak ada di image Airflow. Rebuild image:\n"
            "  docker compose -f airflow/docker-compose.yaml build\n"
            "lalu restart: docker compose -f airflow/docker-compose.yaml up -d"
        )

    client = docker.from_env()
    try:
        container = client.containers.get(SPARK_CONTAINER)
        if container.status != "running":
            raise RuntimeError(
                f"Container '{SPARK_CONTAINER}' status={container.status}. "
                f"Jalankan: docker compose -f docker-compose.spark.yml up -d"
            )
    except docker.errors.NotFound:
        raise RuntimeError(
            f"Container '{SPARK_CONTAINER}' tidak ditemukan. "
            f"Jalankan: docker compose -f docker-compose.spark.yml up -d"
        )

    exit_code, (stdout, stderr) = container.exec_run(
        cmd=["bash", "-lc", cmd],
        workdir="/app",
        environment={"PYTHONPATH": "/app"},
        demux=True,
    )
    if stdout:
        print(stdout.decode(errors="replace"))
    if stderr:
        print(stderr.decode(errors="replace"))
    if exit_code != 0:
        raise RuntimeError(f"Task {task_id} gagal (exit {exit_code}) — lihat log di atas")


# ─── Factories ────────────────────────────────────────────────────────────────

def make_spark_task(task_id: str, job_file: str,
                    extra_args: str = "",
                    driver_memory: str = "1g",
                    timeout_minutes: int = 20):
    """
    Task yang menjalankan spark-submit job di container day43-spark.

    Task: extract_to_hdfs  |  Job: jobs/hive_to_parquet.py --step extract
    """
    cmd = (
        "spark-submit "
        "--master \"${SPARK_MASTER:-local[2]}\" "
        f"--driver-memory {driver_memory} "
        "--conf \"spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}\" "
        "--conf spark.ui.showConsoleProgress=false "
        "--conf spark.sql.adaptive.enabled=true "
        f"--jars {JARS} "
        f"/app/jobs/{job_file} --config {CONFIG} {extra_args}".strip()
    )

    def _run(**context):
        _exec_in_spark_container(cmd, task_id)

    return PythonOperator(
        task_id=task_id,
        python_callable=_run,
        execution_timeout=timedelta(minutes=timeout_minutes),
    )


def make_validation_task(task_id: str, database: str = "adventureworks_curated",
                         tables: list = None, timeout_minutes: int = 5):
    """
    Task row-count check semua tabel curated (Tahap 5 — fan-in).
    Menjalankan jobs/validate_curated.py di container spark.
    """
    tables_arg = ""
    if tables:
        tables_arg = " --tables " + " ".join(tables)

    cmd = (
        f"python /app/jobs/validate_curated.py "
        f"--config {CONFIG} --database {database}{tables_arg}"
    )

    def _run(**context):
        _exec_in_spark_container(cmd, task_id)

    return PythonOperator(
        task_id=task_id,
        python_callable=_run,
        execution_timeout=timedelta(minutes=timeout_minutes),
    )


def make_notify_task(task_id: str = "notify_complete", message: str = None):
    """Tahap 6 — Notify: log ringkasan selesainya pipeline (murni Python)."""

    def _notify(**context):
        dag_run   = context.get("dag_run")
        exec_date = context.get("logical_date") or context.get("execution_date")
        dag_id    = dag_run.dag_id if dag_run else "unknown"

        print(f"\n{'═' * 60}")
        print(f"  PIPELINE COMPLETE")
        print(f"  DAG        : {dag_id}")
        print(f"  Run date   : {exec_date}")
        if message:
            print(f"  Message    : {message}")
        print(f"{'═' * 60}\n")

    return PythonOperator(
        task_id=task_id,
        python_callable=_notify,
    )
