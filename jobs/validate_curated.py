"""
jobs/validate_curated.py
========================
Tahap 5 — Validate: row-count check pada semua tabel curated Hive.

Dipakai oleh:
  - Airflow task `validate_curated` (dag day43_batch_pipeline.py)
  - run_pipeline.sh validate
  - manual: docker exec day43-spark python /app/jobs/validate_curated.py

Usage:
  python jobs/validate_curated.py --config configs/pipeline.yaml
  python jobs/validate_curated.py --config configs/pipeline.yaml \
      --tables adventureworks_curated.fact_sales_performance ...
"""

import argparse
import sys

from utils.config_loader import load_config
from utils.logger import get_logger
from utils.spark_factory import get_spark_with_hive

logger = get_logger(__name__)

# ─── 8 tabel curated sesuai arsitektur pipeline ──────────────────────────────

DEFAULT_TABLES = [
    # sales_pipeline.py
    "fact_sales_performance",
    "monthly_sales_summary",
    "territory_yoy",
    "top_products",
    # hr_pipeline.py
    "fact_hr_workforce",
    # vendor_pipeline.py
    "fact_vendor_performance",
    "vendor_overall_ranking",
    # rfm_pipeline.py
    "fact_customer_rfm",
]


def main():
    parser = argparse.ArgumentParser(
        description="Row-count validation untuk semua tabel curated Hive"
    )
    parser.add_argument("--config", required=True,
                        help="Path to pipeline YAML config")
    parser.add_argument("--database", default="adventureworks_curated",
                        help="Hive database curated (default: adventureworks_curated)")
    parser.add_argument("--tables", nargs="*", default=None,
                        help="Override daftar tabel (default: 8 tabel curated)")
    args = parser.parse_args()

    config = load_config(args.config)
    spark = get_spark_with_hive("ValidateCurated", config.get("spark", {}))

    tables = args.tables
    if not tables:
        tables = [f"{args.database}.{t}" for t in DEFAULT_TABLES]

    print(f"\n{'─' * 60}")
    print(f"Validation: {len(tables)} curated tables")
    print(f"{'─' * 60}")

    failed = []
    for table in tables:
        try:
            cnt = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            status = "✓ OK   " if cnt > 0 else "✗ EMPTY"
            print(f"  {status} | {table} | {cnt:,} rows")
            if cnt == 0:
                failed.append(table)
        except Exception as e:
            print(f"  ✗ ERROR | {table} | {e}")
            failed.append(table)

    spark.stop()

    if failed:
        print(f"\nFAILED: {len(failed)} table(s) missing or empty: {failed}")
        sys.exit(1)

    print(f"\nAll {len(tables)} curated tables validated successfully.\n")


if __name__ == "__main__":
    main()
