"""
Shared module untuk El Niño ETL Case Study.

Berjalan di atas stack lokal peserta:
  - Airflow (LocalExecutor)  -> menjalankan DAG
  - MinIO (object storage)   -> landing zone JSON (raw/open-meteo/...)
  - DuckDB                   -> warehouse bronze / silver / gold

Konfigurasi MinIO dibaca dari Airflow Variables (sama seperti DAG AdventureWorks):
  MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET

Sumber data: Open-Meteo API
  - /v1/forecast   (cuaca ramalan, max 14 hari)
  - /v1/archive    (cuaca historis, 1 tahun ke belakang)
  (/v1/air-quality & /v1/flood bisa dieksplorasi peserta secara mandiri)
"""

import io
import json
import logging
import socket
from datetime import datetime, timedelta

import pandas as pd
import requests
from minio import Minio

log = logging.getLogger(__name__)

# ── MinIO / DuckDB config ──────────────────────────────────────
MINIO_SECURE = False
DUCKDB_PATH  = "/opt/airflow/dags/repo/elnino.duckdb"
OPEN_METEO_TZ = "Asia/Jakarta"

# ── 15 lokasi Indonesia (lat/lon + atribut dimensi) ────────────
# (key, location_name, province, island, latitude, longitude, peatland_flag)
LOCATIONS = [
    ("jakarta",      "jakarta",      "DKI Jakarta",           "Java",            -6.2088,  106.8456, False),
    ("surabaya",     "surabaya",     "Jawa Timur",            "Java",            -7.2575,  112.7521, False),
    ("bandung",      "bandung",      "Jawa Barat",            "Java",            -6.9175,  107.6191, False),
    ("semarang",     "semarang",     "Jawa Tengah",           "Java",            -6.9667,  110.4167, False),
    ("yogyakarta",   "yogyakarta",   "DI Yogyakarta",         "Java",            -7.7956,  110.3695, False),
    ("medan",        "medan",        "Sumatra Utara",         "Sumatra",          3.5952,   98.6722, False),
    ("palembang",    "palembang",    "Sumatra Selatan",       "Sumatra",         -2.9909,  104.7560, False),
    ("pekanbaru",    "pekanbaru",    "Riau",                  "Sumatra",          0.5071,  101.4478, True),
    ("palangkaraya", "palangkaraya", "Kalimantan Tengah",     "Kalimantan",      -2.2096,  113.9200, True),
    ("samarinda",    "samarinda",    "Kalimantan Timur",      "Kalimantan",      -0.5022,  117.1536, True),
    ("makassar",     "makassar",     "Sulawesi Selatan",      "Sulawesi",        -5.1477,  119.4327, False),
    ("manado",       "manado",       "Sulawesi Utara",        "Sulawesi",         1.4748,  124.8421, False),
    ("jayapura",     "jayapura",     "Papua",                 "Papua",           -2.5912,  140.6631, False),
    ("kupang",       "kupang",       "Nusa Tenggara Timur",   "Nusa Tenggara",  -10.1772,  123.6070, False),
    ("mataram",      "mataram",      "Nusa Tenggara Barat",   "Nusa Tenggara",   -8.5833,  116.1167, False),
]

# Variabel cuaca harian yang diambil dari Open-Meteo
DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "et0_fao_evapotranspiration",
    "soil_moisture_0_to_7cm_mean",
]

# Dataset yang diingest (forecast = ramalan, archive = historis)
DATASETS = ["forecast", "archive"]


# ── MinIO helpers ──────────────────────────────────────────────
def _resolve_endpoint(endpoint: str, port: int) -> str:
    host = endpoint.split(":")[0]
    try:
        ip = socket.gethostbyname(host)
        log.info(f"Resolved {host} -> {ip}")
        return f"{ip}:{port}"
    except OSError:
        log.warning(f"Cannot resolve {host}, falling back to {endpoint}")
        return endpoint


def get_minio(endpoint: str, access: str, secret: str) -> Minio:
    ep = _resolve_endpoint(endpoint, 9000)
    return Minio(endpoint=ep, access_key=access, secret_key=secret, secure=MINIO_SECURE)


def upload_json(client: Minio, bucket: str, object_path: str, payload: list) -> None:
    data = json.dumps(payload, default=str).encode("utf-8")
    client.put_object(
        bucket_name=bucket,
        object_name=object_path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    log.info(f"Uploaded -> s3://{bucket}/{object_path} ({len(data)/1024:.1f} KB)")


# ── Open-Meteo fetch ───────────────────────────────────────────
def _build_url(dataset: str, lat: float, lon: float, start: str, end: str) -> str:
    base = {
        "forecast": "https://api.open-meteo.com/v1/forecast",
        "archive": "https://archive-api.open-meteo.com/v1/archive",
    }[dataset]
    var = ",".join(DAILY_VARS)
    if dataset == "forecast":
        return (
            f"{base}?latitude={lat}&longitude={lon}"
            f"&daily={var}&timezone={OPEN_METEO_TZ}&forecast_days=14"
        )
    return (
        f"{base}?latitude={lat}&longitude={lon}"
        f"&daily={var}&timezone={OPEN_METEO_TZ}&start_date={start}&end_date={end}"
    )


def fetch_open_meteo(location: tuple, dataset: str, run_date: str) -> list:
    """
    Ambil data cuaca harian dari Open-Meteo dan flatten menjadi
    list of daily records (1 row per date).
    """
    key, name, province, island, lat, lon, peatland = location
    start = (datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=364)).strftime("%Y-%m-%d")
    url = _build_url(dataset, lat, lon, start, run_date)

    log.info(f"GET {dataset} {name} -> {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    body = resp.json()

    daily = body.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        log.warning(f"{dataset}/{name}: tidak ada data daily")
        return []

    records = []
    for i, d in enumerate(dates):
        rec = {
            "dataset": dataset,
            "location": name,
            "province": province,
            "island": island,
            "latitude": lat,
            "longitude": lon,
            "peatland_flag": peatland,
            "date": d,
            "temperature_2m_max_c": daily.get("temperature_2m_max", [None] * len(dates))[i],
            "temperature_2m_min_c": daily.get("temperature_2m_min", [None] * len(dates))[i],
            "precipitation_sum_mm": daily.get("precipitation_sum", [None] * len(dates))[i],
            "relative_humidity_min_pct": daily.get("relative_humidity_2m_min", [None] * len(dates))[i],
            "wind_speed_10m_max_kmh": daily.get("wind_speed_10m_max", [None] * len(dates))[i],
            "evapotranspiration_mm": daily.get("et0_fao_evapotranspiration", [None] * len(dates))[i],
            "soil_moisture_avg": daily.get("soil_moisture_0_to_7cm_mean", [None] * len(dates))[i],
        }
        # temperature_2m_mean = rata-rata max & min
        tmax, tmin = rec["temperature_2m_max_c"], rec["temperature_2m_min_c"]
        rec["temperature_2m_mean_c"] = round((tmax + tmin) / 2, 3) if tmax is not None and tmin is not None else None
        records.append(rec)
    log.info(f"{dataset}/{name}: {len(records)} daily records")
    return records


# ── DuckDB helpers ─────────────────────────────────────────────
def get_duckdb_conn() -> "duckdb.DuckDBPyConnection":
    import os
    import duckdb

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    return conn


# ── SQL: Silver (fact / dimension) ────────────────────────────
# Setiap tabel di-drop dulu agar pipeline idempoten (re-run aman).
SILVER_SQL = """
-- dim_date
DROP TABLE IF EXISTS silver.dim_date;
CREATE TABLE silver.dim_date AS
SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    EXTRACT(year FROM full_date)::INTEGER   AS year,
    EXTRACT(month FROM full_date)::INTEGER  AS month,
    CASE EXTRACT(month FROM full_date)::INTEGER
        WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
        WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
        WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
        WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
    END AS month_name,
    CASE
        WHEN EXTRACT(month FROM full_date)::INTEGER IN (6,7,8,9) THEN 'dry'
        WHEN EXTRACT(month FROM full_date)::INTEGER IN (12,1,2,3) THEN 'wet'
        ELSE 'transition'
    END AS season_indonesia,
    'El Nino' AS climate_event
FROM (SELECT DISTINCT date::DATE AS full_date FROM bronze.open_meteo_daily);

-- dim_location
DROP TABLE IF EXISTS silver.dim_location;
CREATE TABLE silver.dim_location AS
SELECT
    ROW_NUMBER() OVER (ORDER BY location) AS location_key,
    location      AS location_name,
    province,
    island,
    latitude,
    longitude,
    peatland_flag
FROM (SELECT DISTINCT location, province, island, latitude, longitude, peatland_flag
      FROM bronze.open_meteo_daily);

-- fact_weather_daily (+ crop stress score)
DROP TABLE IF EXISTS silver.fact_weather_daily;
CREATE TABLE silver.fact_weather_daily AS
SELECT
    d.date_key,
    l.location_key,
    b.temperature_2m_mean_c,
    b.temperature_2m_max_c,
    b.precipitation_sum_mm,
    b.relative_humidity_min_pct,
    b.wind_speed_10m_max_kmh,
    b.soil_moisture_avg,
    b.evapotranspiration_mm,
    LEAST(100,
        CASE WHEN b.temperature_2m_max_c >= 38 THEN 40 WHEN b.temperature_2m_max_c >= 35 THEN 30
             WHEN b.temperature_2m_max_c >= 33 THEN 20 WHEN b.temperature_2m_max_c >= 31 THEN 10 ELSE 0 END
      + CASE WHEN b.soil_moisture_avg <= 0.10 THEN 35 WHEN b.soil_moisture_avg <= 0.15 THEN 25
             WHEN b.soil_moisture_avg <= 0.20 THEN 15 WHEN b.soil_moisture_avg <= 0.25 THEN 8 ELSE 0 END
      + CASE WHEN b.evapotranspiration_mm >= 8 THEN 25 WHEN b.evapotranspiration_mm >= 6 THEN 18
             WHEN b.evapotranspiration_mm >= 4 THEN 10 WHEN b.evapotranspiration_mm >= 2 THEN 5 ELSE 0 END
    ) AS crop_stress_score
FROM bronze.open_meteo_daily b
JOIN silver.dim_date d       ON b.date::DATE = d.full_date
JOIN silver.dim_location l   ON b.location = l.location_name;

-- fact_drought_risk_daily
DROP TABLE IF EXISTS silver.fact_drought_risk_daily;
CREATE TABLE silver.fact_drought_risk_daily AS
WITH base AS (
    SELECT
        f.date_key, f.location_key,
        f.temperature_2m_max_c, f.soil_moisture_avg, f.evapotranspiration_mm,
        SUM(f.precipitation_sum_mm) OVER (
            PARTITION BY f.location_key ORDER BY f.date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS precipitation_7d_mm,
        SUM(f.precipitation_sum_mm) OVER (
            PARTITION BY f.location_key ORDER BY f.date_key ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS precipitation_30d_mm,
        SUM(f.evapotranspiration_mm) OVER (
            PARTITION BY f.location_key ORDER BY f.date_key ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS evapotranspiration_30d_mm
    FROM silver.fact_weather_daily f
)
SELECT
    date_key, location_key,
    precipitation_7d_mm, precipitation_30d_mm, soil_moisture_avg, evapotranspiration_30d_mm,
    LEAST(100,
        CASE WHEN precipitation_30d_mm < 30 THEN 25 WHEN precipitation_30d_mm < 60 THEN 18
             WHEN precipitation_30d_mm < 100 THEN 10 WHEN precipitation_30d_mm < 150 THEN 5 ELSE 0 END
      + CASE WHEN temperature_2m_max_c >= 38 THEN 25 WHEN temperature_2m_max_c >= 35 THEN 18
             WHEN temperature_2m_max_c >= 33 THEN 10 WHEN temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
      + CASE WHEN soil_moisture_avg <= 0.10 THEN 25 WHEN soil_moisture_avg <= 0.15 THEN 18
             WHEN soil_moisture_avg <= 0.20 THEN 10 WHEN soil_moisture_avg <= 0.25 THEN 5 ELSE 0 END
      + CASE WHEN evapotranspiration_30d_mm >= 200 THEN 25 WHEN evapotranspiration_30d_mm >= 150 THEN 18
             WHEN evapotranspiration_30d_mm >= 100 THEN 10 WHEN evapotranspiration_30d_mm >= 60 THEN 5 ELSE 0 END
    ) AS drought_risk_score,
    CASE WHEN LEAST(100,
            CASE WHEN precipitation_30d_mm < 30 THEN 25 WHEN precipitation_30d_mm < 60 THEN 18
                 WHEN precipitation_30d_mm < 100 THEN 10 WHEN precipitation_30d_mm < 150 THEN 5 ELSE 0 END
          + CASE WHEN temperature_2m_max_c >= 38 THEN 25 WHEN temperature_2m_max_c >= 35 THEN 18
                 WHEN temperature_2m_max_c >= 33 THEN 10 WHEN temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
          + CASE WHEN soil_moisture_avg <= 0.10 THEN 25 WHEN soil_moisture_avg <= 0.15 THEN 18
                 WHEN soil_moisture_avg <= 0.20 THEN 10 WHEN soil_moisture_avg <= 0.25 THEN 5 ELSE 0 END
          + CASE WHEN evapotranspiration_30d_mm >= 200 THEN 25 WHEN evapotranspiration_30d_mm >= 150 THEN 18
                 WHEN evapotranspiration_30d_mm >= 100 THEN 10 WHEN evapotranspiration_30d_mm >= 60 THEN 5 ELSE 0 END
        ) < 25 THEN 'low'
         WHEN LEAST(100,
            CASE WHEN precipitation_30d_mm < 30 THEN 25 WHEN precipitation_30d_mm < 60 THEN 18
                 WHEN precipitation_30d_mm < 100 THEN 10 WHEN precipitation_30d_mm < 150 THEN 5 ELSE 0 END
          + CASE WHEN temperature_2m_max_c >= 38 THEN 25 WHEN temperature_2m_max_c >= 35 THEN 18
                 WHEN temperature_2m_max_c >= 33 THEN 10 WHEN temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
          + CASE WHEN soil_moisture_avg <= 0.10 THEN 25 WHEN soil_moisture_avg <= 0.15 THEN 18
                 WHEN soil_moisture_avg <= 0.20 THEN 10 WHEN soil_moisture_avg <= 0.25 THEN 5 ELSE 0 END
          + CASE WHEN evapotranspiration_30d_mm >= 200 THEN 25 WHEN evapotranspiration_30d_mm >= 150 THEN 18
                 WHEN evapotranspiration_30d_mm >= 100 THEN 10 WHEN evapotranspiration_30d_mm >= 60 THEN 5 ELSE 0 END
        ) < 50 THEN 'medium'
         WHEN LEAST(100,
            CASE WHEN precipitation_30d_mm < 30 THEN 25 WHEN precipitation_30d_mm < 60 THEN 18
                 WHEN precipitation_30d_mm < 100 THEN 10 WHEN precipitation_30d_mm < 150 THEN 5 ELSE 0 END
          + CASE WHEN temperature_2m_max_c >= 38 THEN 25 WHEN temperature_2m_max_c >= 35 THEN 18
                 WHEN temperature_2m_max_c >= 33 THEN 10 WHEN temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
          + CASE WHEN soil_moisture_avg <= 0.10 THEN 25 WHEN soil_moisture_avg <= 0.15 THEN 18
                 WHEN soil_moisture_avg <= 0.20 THEN 10 WHEN soil_moisture_avg <= 0.25 THEN 5 ELSE 0 END
          + CASE WHEN evapotranspiration_30d_mm >= 200 THEN 25 WHEN evapotranspiration_30d_mm >= 150 THEN 18
                 WHEN evapotranspiration_30d_mm >= 100 THEN 10 WHEN evapotranspiration_30d_mm >= 60 THEN 5 ELSE 0 END
        ) < 75 THEN 'high'
         ELSE 'extreme' END AS drought_risk_level
FROM base;

-- fact_fire_risk_daily (karhutla)
DROP TABLE IF EXISTS silver.fact_fire_risk_daily;
CREATE TABLE silver.fact_fire_risk_daily AS
SELECT
    dr.date_key, dr.location_key,
    dr.drought_risk_score,
    w.temperature_2m_max_c,
    w.relative_humidity_min_pct,
    w.wind_speed_10m_max_kmh,
    dr.precipitation_7d_mm,
    l.peatland_flag,
    LEAST(100,
        dr.drought_risk_score * 0.4
      + CASE WHEN w.temperature_2m_max_c >= 38 THEN 20 WHEN w.temperature_2m_max_c >= 35 THEN 15
             WHEN w.temperature_2m_max_c >= 33 THEN 10 WHEN w.temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
      + CASE WHEN w.relative_humidity_min_pct <= 30 THEN 20 WHEN w.relative_humidity_min_pct <= 40 THEN 15
             WHEN w.relative_humidity_min_pct <= 50 THEN 10 WHEN w.relative_humidity_min_pct <= 60 THEN 5 ELSE 0 END
      + CASE WHEN w.wind_speed_10m_max_kmh >= 30 THEN 15 WHEN w.wind_speed_10m_max_kmh >= 25 THEN 11
             WHEN w.wind_speed_10m_max_kmh >= 20 THEN 7 WHEN w.wind_speed_10m_max_kmh >= 15 THEN 3 ELSE 0 END
      + CASE WHEN l.peatland_flag THEN 5 ELSE 0 END
    ) AS fire_risk_score,
    CASE WHEN LEAST(100,
            dr.drought_risk_score * 0.4
          + CASE WHEN w.temperature_2m_max_c >= 38 THEN 20 WHEN w.temperature_2m_max_c >= 35 THEN 15
                 WHEN w.temperature_2m_max_c >= 33 THEN 10 WHEN w.temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
          + CASE WHEN w.relative_humidity_min_pct <= 30 THEN 20 WHEN w.relative_humidity_min_pct <= 40 THEN 15
                 WHEN w.relative_humidity_min_pct <= 50 THEN 10 WHEN w.relative_humidity_min_pct <= 60 THEN 5 ELSE 0 END
          + CASE WHEN w.wind_speed_10m_max_kmh >= 30 THEN 15 WHEN w.wind_speed_10m_max_kmh >= 25 THEN 11
                 WHEN w.wind_speed_10m_max_kmh >= 20 THEN 7 WHEN w.wind_speed_10m_max_kmh >= 15 THEN 3 ELSE 0 END
          + CASE WHEN l.peatland_flag THEN 5 ELSE 0 END
        ) < 25 THEN 'low'
         WHEN LEAST(100,
            dr.drought_risk_score * 0.4
          + CASE WHEN w.temperature_2m_max_c >= 38 THEN 20 WHEN w.temperature_2m_max_c >= 35 THEN 15
                 WHEN w.temperature_2m_max_c >= 33 THEN 10 WHEN w.temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
          + CASE WHEN w.relative_humidity_min_pct <= 30 THEN 20 WHEN w.relative_humidity_min_pct <= 40 THEN 15
                 WHEN w.relative_humidity_min_pct <= 50 THEN 10 WHEN w.relative_humidity_min_pct <= 60 THEN 5 ELSE 0 END
          + CASE WHEN w.wind_speed_10m_max_kmh >= 30 THEN 15 WHEN w.wind_speed_10m_max_kmh >= 25 THEN 11
                 WHEN w.wind_speed_10m_max_kmh >= 20 THEN 7 WHEN w.wind_speed_10m_max_kmh >= 15 THEN 3 ELSE 0 END
          + CASE WHEN l.peatland_flag THEN 5 ELSE 0 END
        ) < 50 THEN 'medium'
         WHEN LEAST(100,
            dr.drought_risk_score * 0.4
          + CASE WHEN w.temperature_2m_max_c >= 38 THEN 20 WHEN w.temperature_2m_max_c >= 35 THEN 15
                 WHEN w.temperature_2m_max_c >= 33 THEN 10 WHEN w.temperature_2m_max_c >= 31 THEN 5 ELSE 0 END
          + CASE WHEN w.relative_humidity_min_pct <= 30 THEN 20 WHEN w.relative_humidity_min_pct <= 40 THEN 15
                 WHEN w.relative_humidity_min_pct <= 50 THEN 10 WHEN w.relative_humidity_min_pct <= 60 THEN 5 ELSE 0 END
          + CASE WHEN w.wind_speed_10m_max_kmh >= 30 THEN 15 WHEN w.wind_speed_10m_max_kmh >= 25 THEN 11
                 WHEN w.wind_speed_10m_max_kmh >= 20 THEN 7 WHEN w.wind_speed_10m_max_kmh >= 15 THEN 3 ELSE 0 END
          + CASE WHEN l.peatland_flag THEN 5 ELSE 0 END
        ) < 75 THEN 'high'
         ELSE 'extreme' END AS fire_risk_level
FROM silver.fact_drought_risk_daily dr
JOIN silver.fact_weather_daily w ON dr.date_key = w.date_key AND dr.location_key = w.location_key
JOIN silver.dim_location l        ON dr.location_key = l.location_key;

-- fact_mitigation_daily
DROP TABLE IF EXISTS silver.fact_mitigation_daily;
CREATE TABLE silver.fact_mitigation_daily AS
SELECT
    fr.date_key, fr.location_key,
    fr.drought_risk_score,
    fr.fire_risk_score,
    GREATEST(fr.drought_risk_score, fr.fire_risk_score) AS mitigation_priority_score,
    CASE
        WHEN GREATEST(fr.drought_risk_score, fr.fire_risk_score) >= 75 THEN 'Evacuation prep & fire patrol'
        WHEN GREATEST(fr.drought_risk_score, fr.fire_risk_score) >= 50 THEN 'Water allocation & public warning'
        WHEN GREATEST(fr.drought_risk_score, fr.fire_risk_score) >= 25 THEN 'Monitor & early warning'
        ELSE 'Routine monitoring'
    END AS recommended_action
FROM silver.fact_fire_risk_daily fr;
"""

# ── SQL: Gold (dashboard-ready) ───────────────────────────────
GOLD_SQL = """
-- gold.drought_risk_by_region
DROP TABLE IF EXISTS gold.drought_risk_by_region;
CREATE TABLE gold.drought_risk_by_region AS
SELECT
    d.full_date, l.province, l.location_name,
    AVG(f.drought_risk_score) AS avg_drought_risk,
    MAX(f.drought_risk_score) AS peak_drought_risk
FROM silver.fact_drought_risk_daily f
JOIN silver.dim_location l ON f.location_key = l.location_key
JOIN silver.dim_date d     ON f.date_key = d.date_key
GROUP BY d.full_date, l.province, l.location_name;

-- gold.el_nino_weather_trend
DROP TABLE IF EXISTS gold.el_nino_weather_trend;
CREATE TABLE gold.el_nino_weather_trend AS
SELECT
    d.full_date, l.province,
    AVG(f.precipitation_sum_mm) AS avg_rainfall_mm,
    AVG(f.temperature_2m_mean_c) AS avg_temperature_c
FROM silver.fact_weather_daily f
JOIN silver.dim_location l ON f.location_key = l.location_key
JOIN silver.dim_date d     ON f.date_key = d.date_key
GROUP BY d.full_date, l.province;

-- gold.karhutla_risk_by_region
DROP TABLE IF EXISTS gold.karhutla_risk_by_region;
CREATE TABLE gold.karhutla_risk_by_region AS
SELECT
    d.full_date, l.province, l.location_name, l.peatland_flag,
    AVG(f.fire_risk_score) AS avg_fire_risk,
    MAX(f.fire_risk_score) AS peak_fire_risk
FROM silver.fact_fire_risk_daily f
JOIN silver.dim_location l ON f.location_key = l.location_key
JOIN silver.dim_date d     ON f.date_key = d.date_key
GROUP BY d.full_date, l.province, l.location_name, l.peatland_flag;

-- gold.mitigation_priority_daily
DROP TABLE IF EXISTS gold.mitigation_priority_daily;
CREATE TABLE gold.mitigation_priority_daily AS
SELECT
    d.full_date, l.province, l.location_name,
    f.mitigation_priority_score, f.recommended_action
FROM silver.fact_mitigation_daily f
JOIN silver.dim_location l ON f.location_key = l.location_key
JOIN silver.dim_date d     ON f.date_key = d.date_key;
"""
