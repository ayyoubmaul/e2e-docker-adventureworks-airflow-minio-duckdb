import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


# ---------------------------------------------------------
# CALLBACK: alert otomatis kalau task gagal
# ---------------------------------------------------------
def notify_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    logical_date = context["logical_date"]
    log.error(
        "[ALERT] Task '%s' pada DAG '%s' GAGAL pada %s",
        task_id, dag_id, logical_date,
    )
    # TODO: ganti log.error() di atas dengan kirim notifikasi asli (Slack/email)

def notify_retry(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    logical_date = context["logical_date"]
    log.error(
        "[ALERT] Task '%s' pada DAG '%s' HARUS RETRY pada %s",
        task_id, dag_id, logical_date,
    )
def notify_success(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    logical_date = context["logical_date"]
    log.info(
        "[ALERT] Task '%s' pada DAG '%s' SUCCESS pada %s",
        task_id, dag_id, logical_date,
    )

# ---------------------------------------------------------
# 1. Ambil daftar region
#    Di production biasanya ini query ke tabel metadata/config,
#    bukan hardcode - makanya tetap ditambah validasi.
# ---------------------------------------------------------
def get_regions_func():
    regions = ["US", "EU", "APAC", "LATAM", "ASEAN"]
    if not regions:
        raise ValueError("Daftar region kosong - cek sumber data region.")
    return [{"region": r} for r in regions]


# ---------------------------------------------------------
# 2. Proses data per region - generate data asli & tulis ke file asli
#    (bukan cuma print, betulan menghasilkan output yang bisa dicek)
# ---------------------------------------------------------
def process_regional_data_func(target_bucket: str, file_format: str, region: str):
    log.info("Mengambil data untuk region: %s", region)

    raise Exception

    # simulasi data asli - di production ganti dengan query ke DB/warehouse
    df = pd.DataFrame({
        "region": [region] * 5,
        "order_id": range(1, 6),
        "amount": [100, 250, 75, 300, 150],
    })

    output_dir = Path(target_bucket)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{region}.{file_format}"

    if file_format == "parquet":
        df.to_parquet(output_path, index=False)  # perlu: pip install pyarrow
    elif file_format == "csv":
        df.to_csv(output_path, index=False)
    else:
        raise ValueError(f"file_format '{file_format}' belum didukung.")

    log.info("Data %s tersimpan di %s", region, output_path)
    return f"Berhasil memproses {region} -> {output_path}"


# ---------------------------------------------------------
# default_args - retries, callback, dan SLA berlaku ke semua task
# ---------------------------------------------------------
default_args = {
    "owner": "data-eng-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
    "on_success_callback": notify_success,
    # "sla": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_example_alert",
    description="Dynamic task mapping per region - versi real: data asli + logging + callback + SLA",
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "dynamic_mapping", "real"],
) as dag:

    task_get_regions = PythonOperator(
        task_id="get_regions",
        python_callable=get_regions_func,
    )

    task_process_data = PythonOperator.partial(
        task_id="process_regional_data",
        python_callable=process_regional_data_func,
        # target_bucket diganti ke folder lokal supaya bisa langsung dicoba
        # tanpa kredensial cloud - tinggal ganti ke path OSS/bucket asli.
        op_args=["output/global-sales-datalake", "parquet"],
    ).expand(
        op_kwargs=task_get_regions.output
    )

    task_get_regions >> task_process_data
