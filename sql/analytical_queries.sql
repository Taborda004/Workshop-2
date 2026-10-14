-- ============================================================================
-- WORKSHOP-2: ANALYTICAL QUERIES FOR DATA WAREHOUSE & POWER BI
-- Target Database: spotify_grammy_dw (MySQL 8.0)
-- 
-- IMPORTANT (PDF Section 8.1):
-- Every query directly targets the dimensional Data Warehouse (star schema and views).
-- Direct CSV connections are strictly prohibited for the assessed BI solution.
-- ============================================================================

USE spotify_grammy_dw;

-- ============================================================================
-- 1. REQ-01: Impact of Grammy Recognition on Spotify Popularity
-- ============================================================================

-- Query 1.1: Global Popularity Lift and Monotonic Tier Progression
-- Answers: Does Grammy recognition increase streaming popularity, and is the effect cumulative?
SELECT 
    grammy_status,
    nomination_tier,
    total_tracks,
    avg_track_popularity,
    max_track_popularity,
    high_popularity_share_pct
FROM v_kpi_r1_grammy_vs_popularity
ORDER BY 
    CASE nomination_tier
        WHEN '0' THEN 1
        WHEN '1-2' THEN 2
        WHEN '3-5' THEN 3
        WHEN '6-10' THEN 4
        WHEN '>10' THEN 5
        ELSE 6
    END;

-- Query 1.2: Genre-Specific Popularity Lift (Highlighting Commercial Genres)
-- Answers: In which genres is the Grammy popularity boost strongest (> +15 pts)?
SELECT 
    genre_name,
    total_tracks,
    grammy_tracks_count,
    control_tracks_count,
    avg_pop_grammy,
    avg_pop_control,
    popularity_lift
FROM v_kpi_r1_genre_popularity_lift
WHERE popularity_lift > 15.0
ORDER BY popularity_lift DESC;

-- Query 1.3: Direct Fact-Level Lift Verification
SELECT 
    CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS group_name,
    COUNT(f.fact_id) AS track_count,
    ROUND(AVG(f.popularity), 2) AS mean_popularity,
    ROUND(STDDEV(f.popularity), 2) AS std_popularity,
    ROUND(MAX(f.popularity), 2) AS max_popularity
FROM fact_track_performance f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END;


-- ============================================================================
-- 2. REQ-02: Acoustic DNA Profile (9 Musical Dimensions)
-- ============================================================================

-- Query 2.1: Multi-Dimensional Acoustic Radar Profile (Grammy vs Control Group)
-- Answers: What is the acoustic profile distinguishing academy-honored songs from control?
SELECT 
    grammy_status,
    COUNT(*) AS genre_cohort_count,
    ROUND(AVG(avg_danceability), 4) AS danceability,
    ROUND(AVG(avg_energy), 4) AS energy,
    ROUND(AVG(avg_loudness), 2) AS loudness_db,
    ROUND(AVG(avg_speechiness), 4) AS speechiness,
    ROUND(AVG(avg_acousticness), 4) AS acousticness,
    ROUND(AVG(avg_instrumentalness), 4) AS instrumentalness,
    ROUND(AVG(avg_liveness), 4) AS liveness,
    ROUND(AVG(avg_valence), 4) AS valence,
    ROUND(AVG(avg_tempo), 2) AS tempo_bpm
FROM v_kpi_r2_audio_features_by_grammy_genre
GROUP BY grammy_status;

-- Query 2.2: Acoustic Feature Differentials in Commercial vs Alternative Genres
SELECT 
    genre_name,
    grammy_status,
    track_count,
    avg_danceability,
    avg_energy,
    avg_acousticness,
    avg_liveness
FROM v_kpi_r2_audio_features_by_grammy_genre
WHERE genre_name IN ('pop', 'rock', 'hip-hop', 'r-n-b', 'indie', 'folk')
ORDER BY genre_name, grammy_status DESC;


-- ============================================================================
-- 3. REQ-03: Historical Cohorts Persistence vs Control Group
-- ============================================================================

-- Query 3.1: Historical Cohort Performance, High Popularity Share (P75), and CPI
-- Answers: Can older cohorts (1960s, 1970s) sustain 1.8x to 2.4x higher HPS than modern control?
SELECT 
    cohort_decade,
    cohort_group,
    total_tracks,
    high_popularity_tracks,
    hps_p75_pct,
    avg_popularity,
    cohort_persistence_index_cpi
FROM v_kpi_r3_historical_cohorts_performance;

-- Query 3.2: Verification of Empirical Quartile Threshold P75 = 50.0
SELECT 
    'Spotify Catalog P75 Threshold Verification' AS metric_label,
    COUNT(*) AS total_fact_records,
    SUM(CASE WHEN popularity >= 50 THEN 1 ELSE 0 END) AS records_ge_50,
    ROUND(SUM(CASE WHEN popularity >= 50 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS empirical_hps_pct
FROM fact_track_performance;
