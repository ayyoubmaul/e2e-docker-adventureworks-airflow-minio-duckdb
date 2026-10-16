#!/usr/bin/env bash
# run_pipeline.sh
# ===============
# Jalankan pipeline AdventureWorks → Hadoop end-to-end tanpa Airflow,
# dengan cara exec ke container `day43-spark` (docker compose -f docker-compose.spark.yml).
#
# Usage:
#   bash run_pipeline.sh                # full pipeline (all steps)
#   bash run_pipeline.sh extract        # Tahap 1: PostgreSQL → HDFS Parquet
#   bash run_pipeline.sh register       # Tahap 2: register Hive External Tables
#   bash run_pipeline.sh verify         # Tahap 3: row-count validation (raw)
#   bash run_pipeline.sh sales          # Tahap 4a: transform_sales
#   bash run_pipeline.sh hr             # Tahap 4b: transform_hr
#   bash run_pipeline.sh vendor         # Tahap 4c: transform_vendor
#   bash run_pipeline.sh rfm            # Tahap 4d: transform_rfm
#   bash run_pipeline.sh transform      # Tahap 4 (4 transform berurutan)
#   bash run_pipeline.sh validate       # Tahap 5: validate 8 curated tables

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Auto-detect container runtime (docker | podman)
if command -v docker >/dev/null 2>&1; then
    DOCKER_BIN="docker"
elif command -v podman >/dev/null 2>&1; then
    DOCKER_BIN="podman"
else
    echo "ERROR: docker/podman CLI tidak ditemukan di PATH" >&2
    exit 1
fi

SPARK_CONTAINER="${SPARK_CONTAINER:-day43-spark}"
STEP="${1:-all}"
CONFIG="configs/pipeline.yaml"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# ─── Colors ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()     { echo -e "${GREEN}  ✓ $*${NC}"; }
fail()   { echo -e "${RED}  ✗ $*${NC}"; exit 1; }
section(){ echo -e "\n${CYAN}══════════════════════════════════════════════════════${NC}"; \
           echo -e "${CYAN}  $1${NC}"; \
           echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"; \
           echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"; }

# ─── Pre-flight: container spark harus running ────────────────────────────────
if ! "$DOCKER_BIN" ps --format '{{.Names}}' | grep -qx "$SPARK_CONTAINER"; then
    fail "Container '$SPARK_CONTAINER' tidak running. Jalankan: docker compose -f docker-compose.spark.yml up -d"
fi

# ─── spark-submit wrapper (via docker exec) ───────────────────────────────────
run_spark() {
    local job="$1"
    shift
    local log_name
    log_name="$LOG_DIR/${TIMESTAMP}_$(basename "$job" .py).log"

    echo "  → $DOCKER_BIN exec $SPARK_CONTAINER spark-submit $(basename "$job") $*"

    "$DOCKER_BIN" exec "$SPARK_CONTAINER" bash -lc "
        cd /app && PYTHONPATH=/app spark-submit \
            --master \"\${SPARK_MASTER:-local[2]}\" \
            --driver-memory \"\${SPARK_DRIVER_MEMORY:-1g}\" \
            --conf \"spark.sql.shuffle.partitions=\${SPARK_SQL_SHUFFLE_PARTITIONS:-4}\" \
            --conf spark.ui.showConsoleProgress=false \
            --conf spark.sql.adaptive.enabled=true \
            --jars /app/jars/postgresql-42.7.3.jar \
            /app/$job --config $CONFIG $* \
    " 2>&1 | tee "$log_name"

    local exit_code=${PIPESTATUS[0]}
    if [ $exit_code -eq 0 ]; then
        ok "$(basename "$job" .py) finished"
    else
        fail "$(basename "$job" .py) FAILED (exit $exit_code) — see $log_name"
    fi
}

# ─── Individual step functions ────────────────────────────────────────────────

step_extract() {
    section "Tahap 1/5 — Extract: PostgreSQL → HDFS Parquet"
    run_spark jobs/hive_to_parquet.py --step extract
}

step_register() {
    section "Tahap 2/5 — Register Hive External Tables"
    run_spark jobs/hive_to_parquet.py --step register
}

step_verify() {
    section "Tahap 3/5 — Verify Raw Tables (row-count check)"
    run_spark jobs/hive_to_parquet.py --step verify
}

step_sales() {
    section "Tahap 4a/5 — Transform: Sales Performance"
    run_spark jobs/sales_pipeline.py
}

step_hr() {
    section "Tahap 4b/5 — Transform: HR Workforce"
    run_spark jobs/hr_pipeline.py
}

step_vendor() {
    section "Tahap 4c/5 — Transform: Vendor Performance"
    run_spark jobs/vendor_pipeline.py
}

step_rfm() {
    section "Tahap 4d/5 — Transform: RFM Customer Segmentation"
    run_spark jobs/rfm_pipeline.py
}

step_validate() {
    section "Tahap 5/5 — Validate Curated Tables (8 tables)"
    run_spark jobs/validate_curated.py
}

# ─── Route to requested step(s) ───────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   E2E — AdventureWorks Batch Pipeline                ║"
echo "║   Mode: ${STEP}$(printf '%*s' $((47 - ${#STEP})) '')║"
echo "╚══════════════════════════════════════════════════════╝"

START_TIME=$(date +%s)

case "$STEP" in
    all)
        step_extract
        step_register
        step_verify
        step_sales
        step_hr
        step_vendor
        step_rfm
        step_validate
        ;;
    extract)   step_extract   ;;
    register)  step_register  ;;
    verify)    step_verify    ;;
    sales)     step_sales     ;;
    hr)        step_hr        ;;
    vendor)    step_vendor    ;;
    rfm)       step_rfm       ;;
    transform)
        step_sales
        step_hr
        step_vendor
        step_rfm
        ;;
    validate)  step_validate  ;;
    *)
        echo "Unknown step: $STEP"
        echo "Valid options: all | extract | register | verify | sales | hr | vendor | rfm | transform | validate"
        exit 1
        ;;
esac

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Pipeline complete                                  ║"
printf "║   Elapsed: %ds%-40s║\n" "$ELAPSED" ""
echo "║   Logs: logs/${TIMESTAMP}_*.log"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
