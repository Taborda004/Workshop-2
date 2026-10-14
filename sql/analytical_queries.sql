-- ============================================================================
-- WORKSHOP-2: ANALYTICAL QUERIES FOR DATA WAREHOUSE & POWER BI
-- Target Database: spotify_grammy_dw (MySQL 8.0)
-- 
-- IMPORTANT (PDF Section 8.1):
-- Every query directly targets the dimensional Data Warehouse (Star Schema).
-- Direct CSV connections are strictly prohibited for the assessed BI solution.
-- No intermediate pre-aggregated views are used; all metrics are computed
-- dynamically from fact_track_performance and its associated dimensions.
-- ============================================================================

USE spotify_grammy_dw;

-- ============================================================================
-- 1. REQ-01: Impact of Grammy Recognition on Spotify Popularity
-- ============================================================================

-- Query 1.1: Global Popularity Lift and Monotonic Tier Progression
-- Answers: Does Grammy recognition increase streaming popularity, and is the effect cumulative?
SELECT 
    CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS grammy_status,
    a.nomination_tier,
    COUNT(f.fact_id) AS total_tracks,
    ROUND(AVG(f.popularity), 2) AS avg_track_popularity,
    ROUND(MAX(f.popularity), 2) AS max_track_popularity,
    ROUND(AVG(f.is_high_popularity) * 100, 2) AS high_popularity_share_pct
FROM fact_track_performance f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY 
    CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END,
    a.nomination_tier
ORDER BY 
    CASE a.nomination_tier
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
    g.genre_name,
    COUNT(f.fact_id) AS total_tracks,
    COUNT(CASE WHEN a.has_grammy = 1 THEN f.fact_id END) AS grammy_tracks_count,
    COUNT(CASE WHEN a.has_grammy = 0 THEN f.fact_id END) AS control_tracks_count,
    ROUND(AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity ELSE NULL END), 2) AS avg_pop_grammy,
    ROUND(AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity ELSE NULL END), 2) AS avg_pop_control,
    ROUND(
        AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity ELSE NULL END) - 
        AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity ELSE NULL END), 
        2
    ) AS popularity_lift
FROM fact_track_performance f
JOIN dim_genre g ON f.genre_sk = g.genre_sk
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY g.genre_name
HAVING grammy_tracks_count > 0 AND control_tracks_count > 0 AND popularity_lift > 15.0
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
    CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS grammy_status,
    COUNT(f.fact_id) AS track_count,
    ROUND(AVG(f.danceability), 4) AS danceability,
    ROUND(AVG(f.energy), 4) AS energy,
    ROUND(AVG(f.loudness), 2) AS loudness_db,
    ROUND(AVG(f.speechiness), 4) AS speechiness,
    ROUND(AVG(f.acousticness), 4) AS acousticness,
    ROUND(AVG(f.instrumentalness), 4) AS instrumentalness,
    ROUND(AVG(f.liveness), 4) AS liveness,
    ROUND(AVG(f.valence), 4) AS valence,
    ROUND(AVG(f.tempo), 2) AS tempo_bpm
FROM fact_track_performance f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END;

-- Query 2.2: Acoustic Feature Differentials in Commercial vs Alternative Genres
SELECT 
    g.genre_name,
    CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS grammy_status,
    COUNT(f.fact_id) AS track_count,
    ROUND(AVG(f.danceability), 4) AS avg_danceability,
    ROUND(AVG(f.energy), 4) AS avg_energy,
    ROUND(AVG(f.acousticness), 4) AS avg_acousticness,
    ROUND(AVG(f.liveness), 4) AS avg_liveness
FROM fact_track_performance f
JOIN dim_genre g ON f.genre_sk = g.genre_sk
JOIN dim_artist a ON f.artist_sk = a.artist_sk
WHERE g.genre_name IN ('pop', 'rock', 'hip-hop', 'r-n-b', 'indie', 'folk')
GROUP BY g.genre_name, CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END
ORDER BY g.genre_name, grammy_status DESC;


-- ============================================================================
-- 3. REQ-03: Historical Cohorts Persistence vs Control Group
-- ============================================================================

-- Query 3.1: Historical Cohort Performance, High Popularity Share (P75), and CPI
-- Answers: Can older cohorts (1960s, 1970s) sustain 1.8x to 2.4x higher HPS than modern control?
WITH cohort_stats AS (
    SELECT 
        a.first_grammy_decade AS cohort_decade,
        CASE WHEN a.has_grammy = 1 THEN 'Grammy Cohort' ELSE 'Control Group' END AS cohort_group,
        COUNT(f.fact_id) AS total_tracks,
        SUM(f.is_high_popularity) AS high_popularity_tracks,
        ROUND(AVG(f.is_high_popularity) * 100, 2) AS hps_p75_pct,
        ROUND(AVG(f.popularity), 2) AS avg_popularity
    FROM fact_track_performance f
    JOIN dim_artist a ON f.artist_sk = a.artist_sk
    GROUP BY a.first_grammy_decade, CASE WHEN a.has_grammy = 1 THEN 'Grammy Cohort' ELSE 'Control Group' END
),
control_metric AS (
    SELECT MAX(hps_p75_pct) AS control_hps
    FROM cohort_stats
    WHERE cohort_group = 'Control Group' OR cohort_decade = 'Sin Grammy'
)
SELECT 
    c.cohort_decade,
    c.cohort_group,
    c.total_tracks,
    c.high_popularity_tracks,
    c.hps_p75_pct,
    c.avg_popularity,
    ROUND(c.hps_p75_pct / NULLIF((SELECT control_hps FROM control_metric), 0), 2) AS cohort_persistence_index_cpi
FROM cohort_stats c
ORDER BY 
    CASE c.cohort_decade
        WHEN '1950s' THEN 1
        WHEN '1960s' THEN 2
        WHEN '1970s' THEN 3
        WHEN '1980s' THEN 4
        WHEN '1990s' THEN 5
        WHEN '2000s' THEN 6
        WHEN '2010s' THEN 7
        ELSE 8
    END;

-- Query 3.2: Verification of Empirical Quartile Threshold P75 = 50.0
SELECT 
    'Spotify Catalog P75 Threshold Verification' AS metric_label,
    COUNT(*) AS total_fact_records,
    SUM(CASE WHEN popularity >= 50 THEN 1 ELSE 0 END) AS records_ge_50,
    ROUND(SUM(CASE WHEN popularity >= 50 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS empirical_hps_pct
FROM fact_track_performance;
