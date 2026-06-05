import requests
import pandas as pd
import os, time, logging
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
    raise EnvironmentError(
        "SUPABASE_URL tidak ditemukan. "
        "Set sebagai env variable atau isi file .env.\n"
        "Contoh: export SUPABASE_URL='postgresql://user:pass@host:port/db'"
    )

CITIES = [
    {"city": "Surabaya",   "lat": -7.2575,  "lon": 112.7521, "province": "Jawa Timur",       "island": "Jawa",       "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Jakarta",    "lat": -6.2088,  "lon": 106.8456, "province": "DKI Jakarta",      "island": "Jawa",       "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Bandung",    "lat": -6.9175,  "lon": 107.6191, "province": "Jawa Barat",       "island": "Jawa",       "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Medan",      "lat":  3.5952,  "lon":  98.6722, "province": "Sumatra Utara",    "island": "Sumatra",    "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Semarang",   "lat": -6.9932,  "lon": 110.4229, "province": "Jawa Tengah",      "island": "Jawa",       "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Makassar",   "lat": -5.1477,  "lon": 119.4327, "province": "Sulawesi Selatan", "island": "Sulawesi",   "timezone": "WITA", "tz_string": "Asia/Makassar"},
    {"city": "Palembang",  "lat": -2.9761,  "lon": 104.7754, "province": "Sumatra Selatan",  "island": "Sumatra",    "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Denpasar",   "lat": -8.6705,  "lon": 115.2126, "province": "Bali",             "island": "Bali",       "timezone": "WITA", "tz_string": "Asia/Makassar"},
    {"city": "Yogyakarta", "lat": -7.7956,  "lon": 110.3695, "province": "DIY",              "island": "Jawa",       "timezone": "WIB",  "tz_string": "Asia/Jakarta"},
    {"city": "Balikpapan", "lat": -1.2675,  "lon": 116.8289, "province": "Kalimantan Timur", "island": "Kalimantan", "timezone": "WITA", "tz_string": "Asia/Makassar"},
]

API_URL   = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = [
    "temperature_2m", "apparent_temperature", "relative_humidity_2m",
    "dew_point_2m", "precipitation", "rain", "cloud_cover",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "shortwave_radiation", "sunshine_duration", "surface_pressure", "weather_code"
]


DDL_SETUP = """
-- Dimensi
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      INT PRIMARY KEY,
    full_date    DATE,
    day          INT, month INT, year INT,
    quarter      VARCHAR(2), day_of_week VARCHAR(10),
    is_weekend   BOOLEAN, season VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id      INT PRIMARY KEY,
    hour         INT,
    time_of_day  VARCHAR(15),
    is_peak_hour BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id  INT PRIMARY KEY,
    city         VARCHAR(50), province VARCHAR(50),
    island       VARCHAR(30), timezone VARCHAR(10),
    latitude     FLOAT, longitude FLOAT,
    elevation    FLOAT, utc_offset INT
);

-- FIX: nama kolom disesuaikan dengan DataFrame (category_id, weather_code, is_extreme)
CREATE TABLE IF NOT EXISTS dim_weather_category (
    category_id    INT PRIMARY KEY,
    weather_code   INT,
    description    VARCHAR(50),
    severity_level VARCHAR(10),
    is_extreme     BOOLEAN
);

-- Fact table dengan partisi tahunan (OLAP performance)
CREATE TABLE IF NOT EXISTS fact_weather (
    date_id              INT NOT NULL,
    time_id              INT,
    location_id          INT,
    weather_cat_id       INT,
    temperature_2m       FLOAT, apparent_temperature FLOAT,
    relative_humidity_2m FLOAT, dew_point_2m FLOAT,
    precipitation        FLOAT, rain FLOAT,
    cloud_cover          FLOAT, wind_speed_10m FLOAT,
    wind_direction_10m   FLOAT, wind_gusts_10m FLOAT,
    shortwave_radiation  FLOAT, sunshine_duration FLOAT,
    surface_pressure     FLOAT, weather_code INT,
    heat_index_diff      FLOAT,
    rain_intensity       VARCHAR(20), is_rainy_hour BOOLEAN,
    wind_category        VARCHAR(15), direction_label VARCHAR(10),
    is_gust_extreme      BOOLEAN, alert_hour INT,
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

-- Index untuk OLAP query
CREATE INDEX IF NOT EXISTS idx_fw_location ON fact_weather (location_id);
CREATE INDEX IF NOT EXISTS idx_fw_date     ON fact_weather (date_id);
CREATE INDEX IF NOT EXISTS idx_fw_alert    ON fact_weather (alert_hour) WHERE alert_hour = 1;

-- ETL state tracker
CREATE TABLE IF NOT EXISTS etl_state (
    id        INT PRIMARY KEY DEFAULT 1,
    run_ke    INT DEFAULT 0,
    minggu_ke INT DEFAULT 0,
    last_run  TIMESTAMP
);

-- Materialized Views
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_summary AS
SELECT
    d.full_date::date                                    AS tanggal,
    d.year, d.month, d.quarter,
    l.city, l.island, l.province,
    ROUND(AVG(f.temperature_2m)::numeric, 2)             AS avg_temp,
    ROUND(MAX(f.temperature_2m)::numeric, 2)             AS max_temp,
    ROUND(MIN(f.temperature_2m)::numeric, 2)             AS min_temp,
    ROUND(SUM(f.precipitation)::numeric, 2)              AS total_precip,
    ROUND(AVG(f.relative_humidity_2m)::numeric, 2)       AS avg_humidity,
    ROUND(AVG(f.wind_speed_10m)::numeric, 2)             AS avg_wind,
    SUM(f.alert_hour)                                    AS jam_alert,
    COUNT(*) FILTER (WHERE f.is_rainy_hour)              AS jam_hujan
FROM fact_weather f
JOIN dim_date     d ON f.date_id     = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY d.full_date, d.year, d.month, d.quarter, l.city, l.island, l.province
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uix_mvds ON mv_daily_summary (tanggal, city);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_avg AS
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


def setup_db(engine):
    logger.info("SETUP DB...")

    with engine.connect() as conn:
        conn.exec_driver_sql(DDL_SETUP)
        conn.commit()

    logger.info("SETUP DB selesai ✓")



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
            resp = session.get(API_URL, params=params, timeout=(30, 300))
            resp.raise_for_status()
            data = resp.json()
            df = pd.DataFrame(data["hourly"])
            df["city"]      = city["city"]
            df["province"]  = city["province"]
            df["island"]    = city["island"]
            df["timezone"]  = city["timezone"]
            df["latitude"]  = data["latitude"]
            df["longitude"] = data["longitude"]
            df["elevation"] = data["elevation"]
            logger.info(f"  OK: {city['city']} ({len(df)} baris)")
            return df
        except Exception as e:
            logger.warning(f"  {city['city']} gagal percobaan {attempt+1}: {e}")
            time.sleep(10)

    logger.error(f"  GAGAL TOTAL: {city['city']}")
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


def get_weekly_window(state):
    """
    Simulasi periodik: tiap run mengambil 1 minggu data 2025 berikutnya.
    Data 2023–2024 sudah di-load dari Colab (via notebook ETL_DWH_Cuaca_Lengkap).
    """
    minggu_ke = state.get("minggu_ke", 0)
    start_dt  = datetime(2025, 1, 1) + timedelta(weeks=minggu_ke)
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
        if mm == 0:       return "Tidak Hujan"
        elif mm <= 2.5:   return "Hujan Ringan"
        elif mm <= 10:    return "Hujan Sedang"
        elif mm <= 50:    return "Hujan Lebat"
        else:             return "Hujan Ekstrem"

    df['rain_intensity'] = df['precipitation'].apply(rain_intensity)
    df['is_rainy_hour']  = (df['precipitation'] > 0.1)

    def wind_cat(kmh):
        if kmh <= 20:    return "Tenang"
        elif kmh <= 40:  return "Semilir"
        elif kmh <= 60:  return "Kencang"
        else:            return "Berbahaya"

    df['wind_category']   = df['wind_speed_10m'].apply(wind_cat)
    df['gust_ratio']      = df['wind_gusts_10m'] / df['wind_speed_10m'].replace(0, 0.01)
    df['is_gust_extreme'] = (df['gust_ratio'] > 2)
    df['alert_hour']      = ((df['precipitation'] > 5) & (df['wind_speed_10m'] > 20)).astype(int)

    def season(m):
        return {
            12: "Musim Hujan Puncak", 1: "Musim Hujan Puncak", 2: "Musim Hujan Puncak",
             3: "Transisi 1",          4: "Transisi 1",          5: "Transisi 1",
             6: "Musim Kemarau",       7: "Musim Kemarau",       8: "Musim Kemarau"
        }.get(m, "Transisi 2")

    dates    = pd.to_datetime(df['date'].unique())
    dim_date = pd.DataFrame({
        'date_id':     dates.strftime('%Y%m%d').astype(int),
        'full_date':   dates,
        'day':         dates.day,
        'month':       dates.month,
        'year':        dates.year,
        'quarter':     dates.quarter.map({1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}),
        'day_of_week': dates.day_name(),
        'is_weekend':  (dates.day_of_week >= 5),
        'season':      dates.month.map(season)
    }).drop_duplicates('date_id')

    def time_of_day(h):
        if h <= 5:     return "Dini Hari"
        elif h <= 11:  return "Pagi"
        elif h <= 17:  return "Siang"
        else:          return "Malam"

    dim_time = pd.DataFrame({
        'time_id':      range(24),
        'hour':         range(24),
        'time_of_day':  [time_of_day(h) for h in range(24)],
        'is_peak_hour': [h in [7, 8, 9, 17, 18, 19] for h in range(24)]
    })

    loc_cols     = ['city', 'province', 'island', 'timezone', 'latitude', 'longitude', 'elevation']
    dim_location = df[loc_cols].drop_duplicates('city').reset_index(drop=True)
    dim_location['location_id'] = dim_location.index

    tz_offset = {'WIB': 7, 'WITA': 8, 'WIT': 9}
    dim_location['utc_offset'] = dim_location['timezone'].map(tz_offset)

    WMO_MAP = {
         0: ("Cerah",              "Normal",  False),
         1: ("Hampir Cerah",       "Normal",  False),
         2: ("Berawan Sebagian",   "Normal",  False),
         3: ("Mendung",            "Normal",  False),
        45: ("Berkabut",           "Waspada", False),
        48: ("Kabut Beku",         "Waspada", False),
        51: ("Gerimis Ringan",     "Normal",  False),
        61: ("Hujan Ringan",       "Normal",  False),
        63: ("Hujan Sedang",       "Waspada", False),
        65: ("Hujan Lebat",        "Waspada", False),
        80: ("Shower Ringan",      "Normal",  False),
        81: ("Shower Sedang",      "Waspada", False),
        82: ("Shower Lebat",       "Bahaya",  True),
        95: ("Badai Petir",        "Bahaya",  True),
        96: ("Badai Petir+Es",     "Bahaya",  True),
        99: ("Badai Petir Lebat",  "Bahaya",  True),
    }
    dim_weather_category = pd.DataFrame([
        {
            'category_id':    i,
            'weather_code':   k,
            'description':    v[0],
            'severity_level': v[1],
            'is_extreme':     v[2]
        }
        for i, (k, v) in enumerate(WMO_MAP.items())
    ])

    def arah_angin(d):
        if d >= 315 or d < 45:  return "Utara"
        elif d < 135:            return "Timur"
        elif d < 225:            return "Selatan"
        else:                    return "Barat"

    df['direction_label'] = df['wind_direction_10m'].apply(arah_angin)
    df = df.merge(dim_location[['city', 'location_id']], on='city', how='left')

    wmo_to_cat = dim_weather_category.set_index('weather_code')['category_id'].to_dict()
    df['weather_cat_id'] = df['weather_code'].fillna(0).astype(int).map(wmo_to_cat).fillna(0).astype(int)

    FACT_COLS = [
        'date_id', 'time_id', 'location_id', 'weather_cat_id',
        'temperature_2m', 'apparent_temperature', 'relative_humidity_2m',
        'dew_point_2m', 'precipitation', 'rain', 'wind_speed_10m',
        'wind_direction_10m', 'wind_gusts_10m', 'cloud_cover',
        'shortwave_radiation', 'sunshine_duration', 'surface_pressure',
        'weather_code', 'heat_index_diff',
        'rain_intensity', 'is_rainy_hour',
        'wind_category', 'direction_label',
        'is_gust_extreme', 'alert_hour'
    ]

    fact_weather = df.assign(
        date_id = df['time'].dt.strftime('%Y%m%d').astype(int),
        time_id = df['time'].dt.hour,
    )[FACT_COLS].drop_duplicates(subset=['date_id', 'time_id', 'location_id'])

    fact_weather['is_rainy_hour']   = fact_weather['is_rainy_hour'].astype(bool)
    fact_weather['is_gust_extreme'] = fact_weather['is_gust_extreme'].astype(bool)

    logger.info(
        f"  fact_weather: {len(fact_weather):,} baris | "
        f"dim_date: {len(dim_date)} | dim_location: {len(dim_location)}"
    )

    return {
        'fact_weather':         fact_weather,
        'dim_date':             dim_date,
        'dim_time':             dim_time,
        'dim_location':         dim_location,
        'dim_weather_category': dim_weather_category
    }



def load_to_db(tables, engine):
    logger.info("LOAD ke Supabase mulai...")

    def upsert_dim_date(df):
        existing = pd.read_sql("SELECT date_id FROM dim_date", engine)
        new_rows = df[~df["date_id"].isin(existing["date_id"])]
        if len(new_rows) == 0:
            logger.info("  dim_date: tidak ada tanggal baru")
            return
        new_rows.to_sql("dim_date", engine, if_exists="append", index=False)
        logger.info(f"  dim_date: +{len(new_rows)} baris")

    def upsert_dim_static(df, tbl_name):
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {tbl_name}")).scalar()
        if count > 0:
            logger.info(f"  {tbl_name}: sudah ada data, skip")
            return
        df.to_sql(tbl_name, engine, if_exists='append', index=False)
        logger.info(f"  {tbl_name}: +{len(df)} baris")

    def ensure_partition(engine, year: int):
        start = int(f"{year}0101")
        end   = int(f"{year+1}0101")
        table = f"fact_weather_{year}"

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table}
        PARTITION OF fact_weather
        FOR VALUES FROM ({start}) TO ({end});
        """

        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()

        logger.info(f"  Partition checked/created: {table}")

    def append_fact(df, tbl_name, chunksize=500):
        if len(df) == 0:
            logger.info(f"  {tbl_name}: tidak ada data baru")
            return

        years = df['date_id'].astype(str).str[:4].unique()

        for y in years:
            ensure_partition(engine, int(y))

        min_id = int(df['date_id'].min())
        max_id = int(df['date_id'].max())

        with engine.connect() as conn:
            conn.execute(text(
                f"DELETE FROM {tbl_name} WHERE date_id BETWEEN :s AND :e"
            ), {"s": min_id, "e": max_id})
            conn.commit()

        df.to_sql(tbl_name, engine, if_exists='append', index=False, chunksize=500)

        logger.info(f"  {tbl_name}: +{len(df):,} baris")

    upsert_dim_date(tables['dim_date'])
    upsert_dim_static(tables['dim_time'],             'dim_time')
    upsert_dim_static(tables['dim_location'],         'dim_location')
    upsert_dim_static(tables['dim_weather_category'], 'dim_weather_category')
    append_fact(tables['fact_weather'],               'fact_weather')

    with engine.connect() as conn:
        for mv in ["mv_daily_summary", "mv_monthly_avg"]:
            try:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}"))
                conn.commit()
                logger.info(f"  {mv} di-refresh ✓")
            except Exception as e:
                logger.warning(f"  {mv} refresh gagal: {e}")

    logger.info("LOAD selesai ✓")



def load_state(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT run_ke, minggu_ke, last_run FROM etl_state WHERE id = 1"
            )).fetchone()
        if result:
            return {"run_ke": result.run_ke, "minggu_ke": result.minggu_ke, "last_run": str(result.last_run)}
    except Exception:
        pass
    return {"run_ke": 0, "minggu_ke": 0, "last_run": None}


def save_state(engine, state):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO etl_state (id, run_ke, minggu_ke, last_run)
            VALUES (1, :run_ke, :minggu_ke, NOW())
            ON CONFLICT (id) DO UPDATE SET
                run_ke    = EXCLUDED.run_ke,
                minggu_ke = EXCLUDED.minggu_ke,
                last_run  = NOW()
        """), state)
        conn.commit()



def main():
    logger.info("=" * 55)
    logger.info("ETL RUNNER — Data Cuaca Kota Besar Indonesia")
    logger.info("=" * 55)

    engine = create_engine(SUPABASE_URL)

    # setup_db(engine)

    state = load_state(engine)
    start_date, end_date = get_weekly_window(state)
    logger.info(f"Window: {start_date} → {end_date} (run ke-{state['run_ke'] + 1})")

    df_raw = extract_period(start_date, end_date)
    if df_raw is None or len(df_raw) == 0:
        logger.error("Extract gagal, abort.")
        return

    tables = transform(df_raw)
    load_to_db(tables, engine)

    save_state(engine, {
        "run_ke":    state["run_ke"] + 1,
        "minggu_ke": state["minggu_ke"] + 1
    })

    logger.info("=" * 55)
    logger.info(f"ETL SELESAI ✓  (run ke-{state['run_ke'] + 1}, minggu ke-{state['minggu_ke'] + 1})")
    logger.info("=" * 55)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        with open("error_log.txt", "w", encoding="utf-8") as f:
            traceback.print_exc(file=f)
        logger.error("Error tersimpan di error_log.txt")
        raise
