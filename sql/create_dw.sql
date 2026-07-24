-- ============================================
-- DATA WAREHOUSE - QUALITÉ DE L'AIR
-- Modèle en Étoile
-- ============================================

-- ============================================
-- DIMENSION CITY
-- ============================================
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DIMENSION TIME
-- ============================================
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    day_of_week VARCHAR(20),
    day_of_week_num INTEGER,
    month INTEGER,
    year INTEGER,
    is_weekend BOOLEAN DEFAULT FALSE,
    UNIQUE(date, hour)
);

-- Index pour performances
CREATE INDEX IF NOT EXISTS idx_dim_time_date ON dim_time(date);
CREATE INDEX IF NOT EXISTS idx_dim_time_month ON dim_time(month, year);

-- ============================================
-- TABLE FAIT - AIR QUALITY
-- ============================================
CREATE TABLE IF NOT EXISTS fact_air_quality (
    fact_id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(city_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    aqi INTEGER,
    co DECIMAL(10,2),
    no DECIMAL(10,2),
    no2 DECIMAL(10,2),
    o3 DECIMAL(10,2),
    pm2_5 DECIMAL(10,2),
    pm10 DECIMAL(10,2),
    so2 DECIMAL(10,2),
    nh3 DECIMAL(10,2),
    measurement_timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_id, time_id)
);

-- Index pour performances
CREATE INDEX IF NOT EXISTS idx_fact_city ON fact_air_quality(city_id);
CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_air_quality(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_aqi ON fact_air_quality(aqi);
CREATE INDEX IF NOT EXISTS idx_fact_timestamp ON fact_air_quality(measurement_timestamp);

-- ============================================
-- VUES ANALYTIQUES
-- ============================================

-- Vue: AQI moyen par ville (dernière semaine)
CREATE OR REPLACE VIEW vw_aqi_last_week AS
SELECT 
    c.city_name,
    AVG(f.aqi) AS avg_aqi,
    MIN(f.aqi) AS min_aqi,
    MAX(f.aqi) AS max_aqi,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE t.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY c.city_name
ORDER BY avg_aqi DESC;

-- Vue: Alertes (AQI > 150)
CREATE OR REPLACE VIEW vw_alerts AS
SELECT 
    c.city_name,
    t.date,
    t.hour,
    f.aqi,
    f.pm2_5,
    f.pm10,
    f.measurement_timestamp
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.aqi > 150
ORDER BY f.aqi DESC;

-- Vue: Statistiques globales
CREATE OR REPLACE VIEW vw_global_stats AS
SELECT 
    COUNT(*) AS total_mesures,
    COUNT(DISTINCT city_id) AS nb_villes,
    MIN(measurement_timestamp) AS debut,
    MAX(measurement_timestamp) AS fin,
    AVG(aqi) AS aqi_moyen_global,
    MAX(aqi) AS aqi_max
FROM fact_air_quality;