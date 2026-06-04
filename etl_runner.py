import requests
import pandas as pd
import numpy as np
import os, json, time, logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
session = requests.Session()
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
if not SUPABASE_URL:
    raise EnvironmentError("SUPABASE_URL tidak ditemukan. Set sebagai env variable.")

CSV_STAGING  = "raw_all_cities.csv"   
STATE_FILE   = "etl_state.json"       

CITIES = [
    {"city":"Surabaya",   "lat":-7.2575,  "lon":112.7521, "province":"Jawa Timur",       "island":"Jawa",       "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Jakarta",    "lat":-6.2088,  "lon":106.8456, "province":"DKI Jakarta",      "island":"Jawa",       "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Bandung",    "lat":-6.9175,  "lon":107.6191, "province":"Jawa Barat",       "island":"Jawa",       "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Medan",      "lat": 3.5952,  "lon": 98.6722, "province":"Sumatra Utara",    "island":"Sumatra",    "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Semarang",   "lat":-6.9932,  "lon":110.4229, "province":"Jawa Tengah",      "island":"Jawa",       "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Makassar",   "lat":-5.1477,  "lon":119.4327, "province":"Sulawesi Selatan", "island":"Sulawesi",   "timezone":"WITA", "tz_string":"Asia/Makassar"},
    {"city":"Palembang",  "lat":-2.9761,  "lon":104.7754, "province":"Sumatra Selatan",  "island":"Sumatra",    "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Denpasar",   "lat":-8.6705,  "lon":115.2126, "province":"Bali",             "island":"Bali",       "timezone":"WITA", "tz_string":"Asia/Makassar"},
    {"city":"Yogyakarta", "lat":-7.7956,  "lon":110.3695, "province":"DIY",              "island":"Jawa",       "timezone":"WIB",  "tz_string":"Asia/Jakarta"},
    {"city":"Balikpapan", "lat":-1.2675,  "lon":116.8289, "province":"Kalimantan Timur", "island":"Kalimantan", "timezone":"WITA", "tz_string":"Asia/Makassar"},
]

API_URL   = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = [
    "temperature_2m", "apparent_temperature", "relative_humidity_2m",
    "dew_point_2m", "precipitation", "rain", "cloud_cover",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "shortwave_radiation", "sunshine_duration", "surface_pressure", "weather_code"
]

def extract_one_city(city, start_date, end_date):
    params = {
        "latitude":   city["lat"],
        "longitude":  city["lon"],
        "start_date": start_date,
        "end_date":   end_date,
        "hourly":     VARIABLES,
        "timezone":   city["tz_string"]
    }
    for attempt in range(3):
        try:
            resp = session.get(API_URL, params=params,timeout=(30, 300))
            resp.raise_for_status()
            data = resp.json()
            df = pd.DataFrame(data["hourly"])
            df["city"] = city["city"]
            df["province"] = city["province"]
            df["island"] = city["island"]
            df["timezone"] = city["timezone"]
            df["latitude"] = data["latitude"]
            df["longitude"] = data["longitude"]
            df["elevation"] = data["elevation"]
            logger.info(f"OK: {city['city']} ({len(df)} baris)")
            return df

        except Exception as e:
            logger.warning(f"{city['city']} gagal percobaan {attempt+1}: {e}")
            time.sleep(10)

    logger.error(f"GAGAL TOTAL: {city['city']}")
    return None

def extract_period(start_date, end_date):
    logger.info(f"EXTRACT | {start_date} → {end_date}")
    frames = []
    for city in CITIES:
        df = extract_one_city(city, start_date, end_date)
        if df is not None:
            frames.append(df)
        time.sleep(1)  

    if not frames:
        logger.error("Tidak ada data berhasil diambil!")
        return None

    df_all = pd.concat(frames, ignore_index=True)
    logger.info(f"  Total: {len(df_all):,} baris dari {len(frames)} kota")
    return df_all

def get_weekly_window():
    state     = load_state()
    minggu_ke = state.get("minggu_ke", 0)

    start_dt  = datetime(2023, 1, 1) + timedelta(weeks=minggu_ke)
    end_dt    = start_dt + timedelta(days=6)

    today = datetime.utcnow().date()
    if end_dt.date() > today:
        end_dt = datetime.combine(today, datetime.min.time())

    return start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')


def transform(df):
    logger.info("TRANSFORM mulai...")

    df['time'] = pd.to_datetime(df['time'])
    df['date'] = df['time'].dt.date
    df['hour'] = df['time'].dt.hour

    df['precipitation'] = df['precipitation'].clip(lower=0)
    df['rain']          = df['rain'].clip(lower=0)

    df['heat_index_diff'] = df['apparent_temperature'] - df['temperature_2m']

    def rain_intensity(mm):
        if mm == 0:      return "Tidak Hujan"
        elif mm <= 2.5:  return "Hujan Ringan"
        elif mm <= 10:   return "Hujan Sedang"
        elif mm <= 50:   return "Hujan Lebat"
        else:            return "Hujan Ekstrem"

    df['rain_intensity'] = df['precipitation'].apply(rain_intensity)
    df['is_rainy_hour']  = (df['precipitation'] > 0.1).astype(int)

    def wind_cat(kmh):
        if kmh <= 20:   return "Tenang"
        elif kmh <= 40: return "Semilir"
        elif kmh <= 60: return "Kencang"
        else:           return "Berbahaya"

    df['wind_category']  = df['wind_speed_10m'].apply(wind_cat)
    df['gust_ratio']     = df['wind_gusts_10m'] / df['wind_speed_10m'].replace(0, 0.01)
    df['is_gust_extreme']= (df['gust_ratio'] > 2).astype(int)
    df['alert_hour']     = ((df['precipitation'] > 5) & (df['wind_speed_10m'] > 20)).astype(int)

    def season(m):
        return {12:"Musim Hujan Puncak", 1:"Musim Hujan Puncak", 2:"Musim Hujan Puncak",
                3:"Transisi 1", 4:"Transisi 1", 5:"Transisi 1",
                6:"Musim Kemarau", 7:"Musim Kemarau", 8:"Musim Kemarau"}.get(m, "Transisi 2")

    dates    = pd.to_datetime(df['date'].unique())
    dim_date = pd.DataFrame({
        'date_id':     dates.strftime('%Y%m%d').astype(int),
        'full_date':   dates,
        'day':         dates.day,
        'month':       dates.month,
        'year':        dates.year,
        'quarter':     dates.quarter.map({1:'Q1',2:'Q2',3:'Q3',4:'Q4'}),
        'day_of_week': dates.day_name(),
        'is_weekend':  (dates.day_of_week >= 5).astype(int),
        'season':      dates.month.map(season)
    }).drop_duplicates('date_id')

    def time_of_day(h):
        if h <= 5:    return "Dini Hari"
        elif h <= 11: return "Pagi"
        elif h <= 17: return "Siang"
        else:         return "Malam"

    dim_time = pd.DataFrame({
        'time_id':     range(24),
        'hour':        range(24),
        'time_of_day': [time_of_day(h) for h in range(24)],
        'is_peak_hour':[(1 if h in [7,8,9,17,18,19] else 0) for h in range(24)]
    })

    loc_cols = ['city','province','island','timezone','latitude','longitude','elevation']
    dim_location = df[loc_cols].drop_duplicates('city').reset_index(drop=True)
    dim_location['location_id'] = dim_location.index

    tz_offset = {'WIB': 7, 'WITA': 8, 'WIT': 9}
    dim_location['utc_offset'] = dim_location['timezone'].map(tz_offset)

    wmo_map = {
        0:"Cerah", 1:"Cerah Berawan", 2:"Berawan Sebagian", 3:"Berawan Penuh",
        45:"Kabut", 51:"Gerimis Ringan", 53:"Gerimis Sedang", 55:"Gerimis Lebat",
        61:"Hujan Ringan", 63:"Hujan Sedang", 65:"Hujan Lebat",
        80:"Shower Ringan", 81:"Shower Sedang", 82:"Shower Lebat",
        95:"Badai Petir", 96:"Badai Petir + Hujan Es"
    }
    dim_weather_category = pd.DataFrame([
        {'wmo_code': k, 'description': v,
         'is_rain_event': int(k in [51,53,55,61,63,65,80,81,82,95,96]),
         'severity_level': ('Ekstrem' if k >= 95 else 'Lebat' if k in [65,82] else
                            'Sedang' if k in [63,81,53] else 'Ringan' if k > 0 else 'Normal')}
        for k, v in wmo_map.items()
    ])

    df = df.merge(dim_location[['city','location_id']], on='city', how='left')

    fact_weather = df.assign(
        date_id   = df['time'].dt.strftime('%Y%m%d').astype(int),
        time_id   = df['time'].dt.hour,
        wmo_code  = df['weather_code'].fillna(0).astype(int)
    )[[
        'date_id', 'time_id', 'location_id', 'wmo_code',
        'temperature_2m', 'apparent_temperature', 'relative_humidity_2m',
        'dew_point_2m', 'precipitation', 'rain', 'cloud_cover',
        'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
        'shortwave_radiation', 'sunshine_duration', 'surface_pressure',
        'heat_index_diff', 'rain_intensity', 'is_rainy_hour',
        'wind_category', 'gust_ratio', 'is_gust_extreme', 'alert_hour'
    ]].drop_duplicates(subset=['date_id','time_id','location_id'])

    logger.info(f"  fact_weather: {len(fact_weather):,} baris | dim_date: {len(dim_date)} | dim_location: {len(dim_location)}")

    return {
        'fact_weather':         fact_weather,
        'dim_date':             dim_date,
        'dim_time':             dim_time,
        'dim_location':         dim_location,
        'dim_weather_category': dim_weather_category
    }


def load_to_db(tables, engine):
    logger.info("LOAD ke Supabase mulai...")

    def upsert_dim(df, tbl_name):
        df.to_sql(tbl_name, engine, if_exists='append', index=False)
        logger.info(f"  {tbl_name}: {len(df)} baris (append)")

    def append_fact(df, tbl_name, chunksize=5000):
        df.to_sql(tbl_name, engine, if_exists='append', index=False, chunksize=chunksize, method='multi')
        logger.info(f"  {tbl_name}: +{len(df):,} baris (append)")

    with engine.connect() as conn:
        conn.execute(text(DDL_SETUP))
        conn.commit()

    upsert_dim(tables['dim_date'],             'dim_date')
    upsert_dim(tables['dim_time'],             'dim_time')
    upsert_dim(tables['dim_location'],         'dim_location')
    upsert_dim(tables['dim_weather_category'], 'dim_weather_category')
    append_fact(tables['fact_weather'],        'fact_weather')

    with engine.connect() as conn:
        try:
            conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_summary"))
            conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_avg"))
            conn.commit()
            logger.info("  Materialized View di-refresh ✓")
        except Exception as e:
            logger.warning(f"  MV refresh gagal (mungkin belum ada): {e}")

    logger.info("LOAD selesai ✓")


DDL_SETUP = """
-- Dimensi (plain tables)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INT PRIMARY KEY,
    full_date   DATE,
    day         INT, month INT, year INT,
    quarter     VARCHAR(2), day_of_week VARCHAR(10),
    is_weekend  INT, season VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id     INT PRIMARY KEY,
    hour        INT,
    time_of_day VARCHAR(15),
    is_peak_hour INT
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    city        VARCHAR(50), province VARCHAR(50),
    island      VARCHAR(30), timezone VARCHAR(10),
    latitude    FLOAT, longitude FLOAT,
    elevation   FLOAT, utc_offset INT
);

CREATE TABLE IF NOT EXISTS dim_weather_category (
    wmo_code        INT PRIMARY KEY,
    description     VARCHAR(50),
    is_rain_event   INT,
    severity_level  VARCHAR(10)
);

-- Fact table dengan partisi tahunan (OLAP performance)
CREATE TABLE IF NOT EXISTS fact_weather (
    date_id         INT NOT NULL,
    time_id         INT,
    location_id     INT,
    wmo_code        INT,
    temperature_2m  FLOAT, apparent_temperature FLOAT,
    relative_humidity_2m FLOAT, dew_point_2m FLOAT,
    precipitation   FLOAT, rain FLOAT,
    cloud_cover     FLOAT, wind_speed_10m FLOAT,
    wind_direction_10m FLOAT, wind_gusts_10m FLOAT,
    shortwave_radiation FLOAT, sunshine_duration FLOAT,
    surface_pressure FLOAT, heat_index_diff FLOAT,
    rain_intensity  VARCHAR(20), is_rainy_hour INT,
    wind_category   VARCHAR(15), gust_ratio FLOAT,
    is_gust_extreme INT, alert_hour INT,
    PRIMARY KEY (date_id, time_id, location_id)
) PARTITION BY RANGE (date_id);

CREATE TABLE IF NOT EXISTS fact_weather_2023
    PARTITION OF fact_weather FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE IF NOT EXISTS fact_weather_2024
    PARTITION OF fact_weather FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE IF NOT EXISTS fact_weather_2025
    PARTITION OF fact_weather FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE IF NOT EXISTS fact_weather_2026
    PARTITION OF fact_weather FOR VALUES FROM (20260101) TO (20270101);

-- Index untuk OLAP query (lokasi & waktu adalah filter paling umum)
CREATE INDEX IF NOT EXISTS idx_fw_location ON fact_weather (location_id);
CREATE INDEX IF NOT EXISTS idx_fw_date     ON fact_weather (date_id);
CREATE INDEX IF NOT EXISTS idx_fw_alert    ON fact_weather (alert_hour) WHERE alert_hour = 1;

-- Materialized View: ringkasan harian per kota
DROP MATERIALIZED VIEW IF EXISTS mv_daily_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_monthly_avg CASCADE;

CREATE MATERIALIZED VIEW mv_daily_summary AS
SELECT
    d.full_date::date               AS tanggal,
    d.year, d.month, d.quarter,
    l.city, l.island, l.province,
    ROUND(AVG(f.temperature_2m)::numeric, 2)        AS avg_temp,
    ROUND(MAX(f.temperature_2m)::numeric, 2)        AS max_temp,
    ROUND(MIN(f.temperature_2m)::numeric, 2)        AS min_temp,
    ROUND(SUM(f.precipitation)::numeric, 2)         AS total_precip,
    ROUND(AVG(f.relative_humidity_2m)::numeric, 2)  AS avg_humidity,
    ROUND(AVG(f.wind_speed_10m)::numeric, 2)        AS avg_wind,
    SUM(f.alert_hour)                               AS jam_alert,
    SUM(f.is_rainy_hour)                            AS jam_hujan
FROM fact_weather f
JOIN dim_date     d ON f.date_id     = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY d.full_date, d.year, d.month, d.quarter, l.city, l.island, l.province
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uix_mvds ON mv_daily_summary (tanggal, city);

-- Materialized View: rata-rata bulanan
CREATE MATERIALIZED VIEW mv_monthly_avg AS
SELECT
    d.year, d.month, l.city, l.island,
    ROUND(AVG(f.temperature_2m)::numeric, 2)       AS avg_temp,
    ROUND(SUM(f.precipitation)::numeric, 2)        AS total_precip,
    ROUND(AVG(f.relative_humidity_2m)::numeric, 2) AS avg_humidity,
    COUNT(DISTINCT d.date_id)                      AS hari_data
FROM fact_weather f
JOIN dim_date     d ON f.date_id     = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY d.year, d.month, l.city, l.island
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uix_mvma ON mv_monthly_avg (year, month, city);
"""


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"run_ke": 0, "minggu_ke": 0, "last_run": None}

def save_state(state):
    state["last_run"] = datetime.utcnow().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    logger.info(f"  State tersimpan: run #{state['run_ke']}, minggu ke-{state['minggu_ke']}")


def main():
    logger.info("=" * 55)
    logger.info("ETL RUNNER — Data Cuaca Kota Besar Indonesia")
    logger.info("=" * 55)

    state = load_state()

    start_date, end_date = get_weekly_window()
    logger.info(f"Window: {start_date} → {end_date} (run ke-{state['run_ke']+1})")

    df_raw = extract_period(start_date, end_date)
    if df_raw is None or len(df_raw) == 0:
        logger.error("Extract gagal, abort.")
        return

    tables = transform(df_raw)
    engine = create_engine(SUPABASE_URL)
    load_to_db(tables, engine)

    save_state({
        "run_ke":    state["run_ke"] + 1,
        "minggu_ke": state.get("minggu_ke", 0) + 1
    })

    logger.info("=" * 55)
    logger.info("ETL SELESAI ✓")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()