-- ============================================================================
-- WORKSHOP 2 ETL - ANALYTICAL QUERIES FOR DATA WAREHOUSE VALIDATION & REPORTING
-- Database: spotify_grammy_dw (MySQL 8.0)
-- Grain: Track x Genre (Compound Key: track_id, genre_sk)
-- ============================================================================

USE spotify_grammy_dw;

-- ============================================================================
-- 1. REQ-01: GRAMMY RECOGNITION VS. POPULARITY ACROSS GENRES & NOMINATIONS
-- Question: ¿Existe una asociación entre el reconocimiento registrado en los
-- Grammy Awards y la popularidad de los artistas en Spotify, y cómo varía esta
-- relación según el género musical y la cantidad de reconocimientos?
-- ============================================================================

-- Query 1.1: Overall Popularity Comparison by Grammy Recognition Tier
SELECT 
    da.has_grammy,
    CASE 
        WHEN da.has_grammy = 0 THEN '0. Sin Reconocimiento (Grupo Control)'
        WHEN da.total_nominations BETWEEN 1 AND 2 THEN '1. 1-2 Nominaciones'
        WHEN da.total_nominations BETWEEN 3 AND 5 THEN '2. 3-5 Nominaciones'
        WHEN da.total_nominations BETWEEN 6 AND 10 THEN '3. 6-10 Nominaciones'
        ELSE '4. Más de 10 Nominaciones'
    END AS recognition_tier,
    COUNT(DISTINCT ft.track_id) AS total_tracks,
    COUNT(DISTINCT da.artist_sk) AS total_artists,
    ROUND(AVG(ft.popularity), 2) AS avg_popularity,
    ROUND(STDDEV(ft.popularity), 2) AS std_popularity,
    ROUND(100.0 * SUM(CASE WHEN ft.popularity >= 50 THEN 1 ELSE 0 END) / COUNT(*), 2) AS high_popularity_share_pct
FROM fact_track_performance ft
JOIN dim_artist da ON ft.artist_sk = da.artist_sk
GROUP BY da.has_grammy, recognition_tier
ORDER BY da.has_grammy, recognition_tier;

-- Query 1.2: Popularity Lift by Genre (Grammy Recognized vs Comparison Group)
SELECT 
    dg.genre_name,
    COUNT(CASE WHEN da.has_grammy = 1 THEN 1 END) AS grammy_tracks_count,
    COUNT(CASE WHEN da.has_grammy = 0 THEN 1 END) AS comparison_tracks_count,
    ROUND(AVG(CASE WHEN da.has_grammy = 1 THEN ft.popularity END), 2) AS avg_pop_grammy,
    ROUND(AVG(CASE WHEN da.has_grammy = 0 THEN ft.popularity END), 2) AS avg_pop_comparison,
    ROUND(
        AVG(CASE WHEN da.has_grammy = 1 THEN ft.popularity END) - 
        AVG(CASE WHEN da.has_grammy = 0 THEN ft.popularity END), 2
    ) AS popularity_lift
FROM fact_track_performance ft
JOIN dim_genre dg ON ft.genre_sk = dg.genre_sk
JOIN dim_artist da ON ft.artist_sk = da.artist_sk
GROUP BY dg.genre_name
HAVING grammy_tracks_count >= 20
ORDER BY popularity_lift DESC
LIMIT 20;


-- ============================================================================
-- 2. REQ-02: PREDOMINANT AUDIO FEATURES BY GRAMMY RECOGNITION & GENRE
-- Question: ¿Cuáles son las características musicales predominantes en las
-- canciones de Spotify asociadas con artistas reconocidos en los Grammy y cómo
-- varían según el género musical?
-- ============================================================================

-- Query 2.1: Macro Audio Profiles: Grammy Recognized vs. Comparison Group
SELECT 
    CASE WHEN da.has_grammy = 1 THEN 'Grammy Recognized' ELSE 'Comparison Group' END AS artist_cohort,
    COUNT(*) AS total_track_genres,
    ROUND(AVG(ft.danceability), 4) AS avg_danceability,
    ROUND(AVG(ft.energy), 4) AS avg_energy,
    ROUND(AVG(ft.loudness), 2) AS avg_loudness_db,
    ROUND(AVG(ft.speechiness), 4) AS avg_speechiness,
    ROUND(AVG(ft.acousticness), 4) AS avg_acousticness,
    ROUND(AVG(ft.instrumentalness), 4) AS avg_instrumentalness,
    ROUND(AVG(ft.liveness), 4) AS avg_liveness,
    ROUND(AVG(ft.valence), 4) AS avg_valence,
    ROUND(AVG(ft.tempo), 1) AS avg_tempo_bpm,
    ROUND(AVG(ft.duration_ms) / 1000.0, 1) AS avg_duration_sec
FROM fact_track_performance ft
JOIN dim_artist da ON ft.artist_sk = da.artist_sk
GROUP BY da.has_grammy;

-- Query 2.2: Audio Differences by Genre in Top Genres
SELECT 
    dg.genre_name,
    ROUND(AVG(CASE WHEN da.has_grammy = 1 THEN ft.danceability END) - AVG(CASE WHEN da.has_grammy = 0 THEN ft.danceability END), 3) AS diff_danceability,
    ROUND(AVG(CASE WHEN da.has_grammy = 1 THEN ft.energy END) - AVG(CASE WHEN da.has_grammy = 0 THEN ft.energy END), 3) AS diff_energy,
    ROUND(AVG(CASE WHEN da.has_grammy = 1 THEN ft.acousticness END) - AVG(CASE WHEN da.has_grammy = 0 THEN ft.acousticness END), 3) AS diff_acousticness,
    ROUND(AVG(CASE WHEN da.has_grammy = 1 THEN ft.valence END) - AVG(CASE WHEN da.has_grammy = 0 THEN ft.valence END), 3) AS diff_valence
FROM fact_track_performance ft
JOIN dim_genre dg ON ft.genre_sk = dg.genre_sk
JOIN dim_artist da ON ft.artist_sk = da.artist_sk
GROUP BY dg.genre_name
HAVING COUNT(CASE WHEN da.has_grammy = 1 THEN 1 END) >= 30
ORDER BY dg.genre_name;


-- ============================================================================
-- 3. REQ-03: HISTORICAL COHORTS & POPULARITY PERSISTENCE (P75 THRESHOLD)
-- Question: ¿Los artistas con reconocimiento registrado en los Grammy presentan
-- una mayor popularidad en Spotify que los artistas sin reconocimiento registrado,
-- o los artistas del grupo de comparación alcanzan niveles iguales o superiores?
-- ============================================================================

-- Query 3.1: Historical Cohort Performance by First Grammy Decade
SELECT 
    COALESCE(da.first_grammy_decade, 'Sin Grammy') AS first_recognition_decade,
    COUNT(DISTINCT da.artist_sk) AS total_artists,
    COUNT(DISTINCT ft.track_id) AS total_tracks,
    ROUND(AVG(ft.popularity), 2) AS avg_popularity,
    -- P75 Threshold is 50.0 based on statistical data profiling
    ROUND(100.0 * SUM(CASE WHEN ft.popularity >= 50 THEN 1 ELSE 0 END) / COUNT(*), 2) AS high_popularity_share_pct,
    ROUND(
        (100.0 * SUM(CASE WHEN ft.popularity >= 50 THEN 1 ELSE 0 END) / COUNT(*)) / 
        (SELECT 100.0 * SUM(CASE WHEN ft_c.popularity >= 50 THEN 1 ELSE 0 END) / COUNT(*) 
         FROM fact_track_performance ft_c 
         JOIN dim_artist da_c ON ft_c.artist_sk = da_c.artist_sk 
         WHERE da_c.has_grammy = 0), 2
    ) AS cohort_persistence_index_cpi
FROM fact_track_performance ft
JOIN dim_artist da ON ft.artist_sk = da.artist_sk
GROUP BY COALESCE(da.first_grammy_decade, 'Sin Grammy')
ORDER BY 
    CASE 
        WHEN first_recognition_decade = 'Sin Grammy' THEN '9999'
        ELSE first_recognition_decade
    END;


-- ============================================================================
-- 4. VERIFICATION OF PRECOMPUTED KPI VIEWS (FOR POWER BI DIRECT QUERY / IMPORT)
-- ============================================================================

-- View 1 Check:
SELECT * FROM v_kpi_r1_grammy_vs_popularity LIMIT 10;

-- View 2 Check:
SELECT * FROM v_kpi_r1_genre_popularity_lift ORDER BY popularity_lift DESC LIMIT 10;

-- View 3 Check:
SELECT * FROM v_kpi_r2_audio_features_by_grammy_genre LIMIT 10;

-- View 4 Check:
SELECT * FROM v_kpi_r3_historical_cohorts_performance;

-- View 5 Check:
SELECT * FROM v_kpi_artist_summary ORDER BY total_tracks DESC LIMIT 10;
