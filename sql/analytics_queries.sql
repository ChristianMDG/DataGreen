-- ============================================
-- ANALYTICS_QUERIES.SQL
-- Requêtes d'analyse pour le Data Warehouse
-- Qualité de l'Air - 5 villes européennes
-- ============================================

-- ============================================
-- 1. STATISTIQUES GLOBALES
-- ============================================

-- 1.1 Vue d'ensemble des données
SELECT 
    COUNT(*) AS total_mesures,
    COUNT(DISTINCT city_id) AS nb_villes,
    COUNT(DISTINCT time_id) AS nb_temps,
    MIN(measurement_timestamp) AS premiere_mesure,
    MAX(measurement_timestamp) AS derniere_mesure,
    AVG(aqi) AS aqi_moyen_global,
    MAX(aqi) AS aqi_max_global
FROM fact_air_quality;

-- 1.2 Répartition par ville
SELECT 
    c.city_name,
    COUNT(*) AS nb_mesures,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max,
    ROUND(AVG(f.pm2_5), 2) AS pm25_moyen,
    ROUND(AVG(f.pm10), 2) AS pm10_moyen,
    ROUND(AVG(f.no2), 2) AS no2_moyen
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name
ORDER BY aqi_moyen DESC;

-- 1.3 Statistiques par mois
SELECT 
    t.year,
    t.month,
    COUNT(*) AS nb_mesures,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year, t.month
ORDER BY t.year DESC, t.month DESC;

-- 1.4 Statistiques par saison
SELECT 
    CASE 
        WHEN t.month IN (12, 1, 2) THEN 'Hiver'
        WHEN t.month IN (3, 4, 5) THEN 'Printemps'
        WHEN t.month IN (6, 7, 8) THEN 'Été'
        ELSE 'Automne'
    END AS saison,
    COUNT(*) AS nb_mesures,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY saison
ORDER BY aqi_moyen DESC;

-- ============================================
-- 2. ANALYSE TEMPORELLE
-- ============================================

-- 2.1 Évolution quotidienne de l'AQI (30 derniers jours)
SELECT 
    c.city_name,
    t.date,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE t.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.city_name, t.date
ORDER BY t.date DESC, aqi_moyen DESC;

-- 2.2 Évolution par heure de la journée
SELECT 
    t.hour,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.hour
ORDER BY t.hour;

-- 2.3 AQI par jour de la semaine
SELECT 
    t.day_of_week,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.day_of_week
ORDER BY aqi_moyen DESC;

-- 2.4 Weekend vs Semaine
SELECT 
    CASE 
        WHEN t.is_weekend = TRUE THEN 'Weekend'
        ELSE 'Semaine'
    END AS periode,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    MIN(f.aqi) AS aqi_min,
    MAX(f.aqi) AS aqi_max,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY periode;

-- 2.5 Comparaison par ville et heure
SELECT 
    c.city_name,
    t.hour,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name, t.hour
ORDER BY c.city_name, t.hour;

-- ============================================
-- 3. ANALYSE DES POLLUANTS
-- ============================================

-- 3.1 Moyenne des polluants par ville
SELECT 
    c.city_name,
    ROUND(AVG(f.pm2_5), 2) AS avg_pm25,
    ROUND(AVG(f.pm10), 2) AS avg_pm10,
    ROUND(AVG(f.no2), 2) AS avg_no2,
    ROUND(AVG(f.o3), 2) AS avg_o3,
    ROUND(AVG(f.co), 2) AS avg_co,
    ROUND(AVG(f.so2), 2) AS avg_so2,
    ROUND(AVG(f.nh3), 2) AS avg_nh3
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name
ORDER BY c.city_name;

-- 3.2 Corrélation entre polluants
SELECT 
    CORR(pm2_5, pm10) AS pm25_pm10_corr,
    CORR(pm2_5, no2) AS pm25_no2_corr,
    CORR(no2, o3) AS no2_o3_corr,
    CORR(co, no2) AS co_no2_corr,
    CORR(pm10, so2) AS pm10_so2_corr
FROM fact_air_quality;

-- 3.3 Ratio PM2.5/PM10
SELECT 
    c.city_name,
    ROUND(AVG(f.pm2_5 / NULLIF(f.pm10, 0)), 2) AS ratio_pm25_pm10,
    ROUND(AVG(f.pm2_5), 2) AS avg_pm25,
    ROUND(AVG(f.pm10), 2) AS avg_pm10
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name
ORDER BY ratio_pm25_pm10 DESC;

-- ============================================
-- 4. ALERTES ET SEUILS CRITIQUES
-- ============================================

-- 4.1 Alertes AQI > 150 (mauvais)
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
ORDER BY f.aqi DESC
LIMIT 50;

-- 4.2 Nombre d'alertes par ville
SELECT 
    c.city_name,
    COUNT(*) AS nb_alertes,
    MIN(f.aqi) AS aqi_min_alerte,
    MAX(f.aqi) AS aqi_max_alerte,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen_alerte
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
WHERE f.aqi > 150
GROUP BY c.city_name
ORDER BY nb_alertes DESC;

-- 4.3 Alertes PM2.5 > 50 μg/m³
SELECT 
    c.city_name,
    COUNT(*) AS nb_alertes_pm25,
    MIN(f.pm2_5) AS pm25_min,
    MAX(f.pm2_5) AS pm25_max,
    ROUND(AVG(f.pm2_5), 2) AS pm25_moyen
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
WHERE f.pm2_5 > 50
GROUP BY c.city_name
ORDER BY nb_alertes_pm25 DESC;

-- 4.4 Jours avec la pire qualité de l'air
SELECT 
    c.city_name,
    t.date,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen_jour,
    MAX(f.aqi) AS aqi_max_jour,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name, t.date
ORDER BY aqi_moyen_jour DESC
LIMIT 20;

-- ============================================
-- 5. ANALYSE COMPARATIVE
-- ============================================

-- 5.1 Comparaison des villes (AQI moyen)
SELECT 
    c.city_name,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen,
    ROUND(AVG(f.aqi) - (SELECT AVG(aqi) FROM fact_air_quality), 2) AS ecart_moyenne
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name
ORDER BY aqi_moyen DESC;

-- 5.2 Top 5 des meilleures heures par ville
SELECT 
    c.city_name,
    t.hour,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name, t.hour
ORDER BY c.city_name, aqi_moyen
LIMIT 20;

-- 5.3 Top 5 des pires heures par ville
SELECT 
    c.city_name,
    t.hour,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name, t.hour
ORDER BY c.city_name, aqi_moyen DESC
LIMIT 20;

-- 5.4 Comparaison mensuelle par ville
SELECT 
    c.city_name,
    t.year,
    t.month,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen_mensuel
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name, t.year, t.month
ORDER BY c.city_name, t.year, t.month;

-- ============================================
-- 6. ANALYSE DES TENDANCES
-- ============================================

-- 6.1 Moyenne mobile (7 jours) par ville
WITH daily_avg AS (
    SELECT 
        c.city_name,
        t.date,
        ROUND(AVG(f.aqi), 2) AS aqi_jour
    FROM fact_air_quality f
    JOIN dim_city c ON f.city_id = c.city_id
    JOIN dim_time t ON f.time_id = t.time_id
    GROUP BY c.city_name, t.date
)
SELECT 
    city_name,
    date,
    aqi_jour,
    ROUND(AVG(aqi_jour) OVER (PARTITION BY city_name ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moyenne_mobile_7j
FROM daily_avg
ORDER BY city_name, date DESC
LIMIT 50;

-- 6.2 Évolution du nombre de mesures par mois
SELECT 
    t.year,
    t.month,
    COUNT(*) AS nb_mesures,
    ROUND((COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY t.year, t.month)) * 100.0 / NULLIF(LAG(COUNT(*)) OVER (ORDER BY t.year, t.month), 0), 2) AS variation_pct
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year, t.month
ORDER BY t.year DESC, t.month DESC;

-- 6.3 Comparaison année sur année (12 mois)
SELECT 
    c.city_name,
    EXTRACT(MONTH FROM t.date) AS mois,
    ROUND(AVG(f.aqi), 2) AS aqi_moyen
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE t.date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY c.city_name, EXTRACT(MONTH FROM t.date)
ORDER BY c.city_name, mois;

-- ============================================
-- 7. EXPORT DES DONNÉES
-- ============================================

-- 7.1 Export complet (toutes les données)
SELECT 
    c.city_name,
    t.date,
    t.hour,
    t.day_of_week,
    f.aqi,
    f.pm2_5,
    f.pm10,
    f.no2,
    f.o3,
    f.co,
    f.so2,
    f.nh3,
    f.measurement_timestamp
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
ORDER BY t.date, t.hour, c.city_name;

-- 7.2 Export par ville
COPY (
    SELECT 
        t.date,
        t.hour,
        f.aqi,
        f.pm2_5,
        f.pm10,
        f.no2,
        f.o3
    FROM fact_air_quality f
    JOIN dim_city c ON f.city_id = c.city_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE c.city_name = 'Paris'
    ORDER BY t.date, t.hour
) TO '/tmp/paris_air_quality.csv' CSV HEADER;

-- 7.3 Export des alertes
COPY (
    SELECT 
        c.city_name,
        t.date,
        t.hour,
        f.aqi,
        f.pm2_5,
        f.pm10
    FROM fact_air_quality f
    JOIN dim_city c ON f.city_id = c.city_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.aqi > 150
    ORDER BY f.aqi DESC
) TO '/tmp/air_quality_alerts.csv' CSV HEADER;

-- ============================================
-- 8. VUES ANALYTIQUES
-- ============================================

-- 8.1 Création de la vue AQI par ville (dernière semaine)
CREATE OR REPLACE VIEW vw_aqi_last_week AS
SELECT 
    c.city_name,
    ROUND(AVG(f.aqi), 2) AS avg_aqi,
    MIN(f.aqi) AS min_aqi,
    MAX(f.aqi) AS max_aqi,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE t.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY c.city_name
ORDER BY avg_aqi DESC;

-- 8.2 Création de la vue Alertes
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

-- 8.3 Création de la vue Statistiques globales
CREATE OR REPLACE VIEW vw_global_stats AS
SELECT 
    COUNT(*) AS total_mesures,
    COUNT(DISTINCT city_id) AS nb_villes,
    MIN(measurement_timestamp) AS debut,
    MAX(measurement_timestamp) AS fin,
    ROUND(AVG(aqi), 2) AS aqi_moyen_global,
    MAX(aqi) AS aqi_max,
    ROUND(AVG(pm2_5), 2) AS pm25_moyen_global,
    ROUND(AVG(pm10), 2) AS pm10_moyen_global
FROM fact_air_quality;

-- 8.4 Création de la vue Classement des villes
CREATE OR REPLACE VIEW vw_city_ranking AS
SELECT 
    c.city_name,
    ROUND(AVG(f.aqi), 2) AS avg_aqi,
    RANK() OVER (ORDER BY AVG(f.aqi) DESC) AS aqi_rank
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name
ORDER BY avg_aqi DESC;

-- 8.5 Création de la vue Analyse horaire
CREATE OR REPLACE VIEW vw_hourly_analysis AS
SELECT 
    t.hour,
    ROUND(AVG(f.aqi), 2) AS avg_aqi,
    MIN(f.aqi) AS min_aqi,
    MAX(f.aqi) AS max_aqi,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.hour
ORDER BY t.hour;

-- ============================================
-- 9. REQUÊTES D'INTÉGRITÉ
-- ============================================

-- 9.1 Vérification de la cohérence des données
SELECT 
    (SELECT COUNT(*) FROM fact_air_quality) AS nb_faits,
    (SELECT COUNT(DISTINCT city_id) FROM dim_city) * (SELECT COUNT(DISTINCT time_id) FROM dim_time) AS nb_attendus,
    ROUND((SELECT COUNT(*) FROM fact_air_quality) * 100.0 / NULLIF((SELECT COUNT(DISTINCT city_id) FROM dim_city) * (SELECT COUNT(DISTINCT time_id) FROM dim_time), 0), 2) AS coherence_pct;

-- 9.2 Détection des trous dans les données
SELECT 
    c.city_name,
    t.date,
    COUNT(*) AS nb_mesures
FROM fact_air_quality f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.city_name, t.date
HAVING COUNT(*) < 12  -- Moins de 12 heures dans la journée
ORDER BY nb_mesures;

-- 9.3 Détection des doublons
SELECT 
    city_id, 
    time_id, 
    COUNT(*) AS nb_doublons
FROM fact_air_quality
GROUP BY city_id, time_id
HAVING COUNT(*) > 1;