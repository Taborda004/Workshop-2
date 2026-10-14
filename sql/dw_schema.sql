-- ============================================================================
-- WORKSHOP-2: DIMENSIONAL DATA WAREHOUSE SCHEMA (MySQL 8.0)
-- Target Database: spotify_grammy_dw
-- Purpose: Star Schema and Analytical Views supporting REQ-01, REQ-02, REQ-03
--
-- IMPORTANT ARCHITECTURAL DISTINCTION (PDF Section 6.10):
-- This script creates the analytical Data Warehouse loaded by the ETL pipeline
-- (load_dw task), completely separate from the operational source database.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS spotify_grammy_dw
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE spotify_grammy_dw;

-- Disable foreign key checks for clean teardown during schema setup
SET FOREIGN_KEY_CHECKS = 0;

DROP VIEW IF EXISTS v_kpi_r3_historical_cohorts_performance;
DROP VIEW IF EXISTS v_kpi_r2_audio_features_by_grammy_genre;
DROP VIEW IF EXISTS v_kpi_r1_genre_popularity_lift;
DROP VIEW IF EXISTS v_kpi_r1_grammy_vs_popularity;

DROP TABLE IF EXISTS fact_track_performance;
DROP TABLE IF EXISTS dim_artist;
DROP TABLE IF EXISTS dim_genre;
DROP TABLE IF EXISTS dim_date;

SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- 1. DIMENSION TABLES
-- ============================================================================

-- 1.1 Artist Dimension
CREATE TABLE dim_artist (
    artist_sk INT AUTO_INCREMENT PRIMARY KEY,
    artist_name VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    has_grammy TINYINT(1) NOT NULL DEFAULT 0,
    total_nominations INT NOT NULL DEFAULT 0,
    total_wins INT NOT NULL DEFAULT 0,
    nomination_tier VARCHAR(20) NOT NULL DEFAULT '0',
    first_grammy_year INT NULL,
    first_grammy_decade VARCHAR(20) NOT NULL DEFAULT 'Sin Grammy',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_artist_name (artist_name),
    INDEX idx_artist_has_grammy (has_grammy),
    INDEX idx_artist_tier (nomination_tier),
    INDEX idx_artist_decade (first_grammy_decade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 1.2 Genre Dimension
CREATE TABLE dim_genre (
    genre_sk INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_genre_name (genre_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 1.3 Date / Decade Dimension (Historical Cohorts Context)
CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,
    award_year INT NOT NULL,
    decade VARCHAR(20) NOT NULL,
    is_grammy_era TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date_decade (decade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 2. FACT TABLE: Track Performance
-- Grain: One row represents the performance of a Song in a Specific Genre
-- Exact Expected Volume: 113,549 rows
-- ============================================================================
CREATE TABLE fact_track_performance (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    track_id VARCHAR(100) NOT NULL,
    artist_sk INT NOT NULL,
    genre_sk INT NOT NULL,
    date_sk INT NULL,
    track_name VARCHAR(1000) NOT NULL,
    album_name VARCHAR(1000) NULL,
    popularity INT NOT NULL,
    is_high_popularity TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 if popularity >= 50.0 (P75 empirical threshold for REQ-03)',
    duration_ms INT NOT NULL,
    explicit TINYINT(1) NOT NULL DEFAULT 0,
    -- 9 Audio Dimensions for REQ-02
    danceability DOUBLE NOT NULL,
    energy DOUBLE NOT NULL,
    key_signature INT NOT NULL,
    loudness DOUBLE NOT NULL,
    audio_mode INT NOT NULL,
    speechiness DOUBLE NOT NULL,
    acousticness DOUBLE NOT NULL,
    instrumentalness DOUBLE NOT NULL,
    liveness DOUBLE NOT NULL,
    valence DOUBLE NOT NULL,
    tempo DOUBLE NOT NULL,
    time_signature INT NOT NULL,
    has_grammy TINYINT(1) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Foreign key constraints
    CONSTRAINT fk_fact_artist FOREIGN KEY (artist_sk) REFERENCES dim_artist (artist_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_genre FOREIGN KEY (genre_sk) REFERENCES dim_genre (genre_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_date FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    -- Unique Grain Constraint
    UNIQUE KEY uq_track_genre (track_id, genre_sk),
    -- Performance Indexes
    INDEX idx_fact_popularity (popularity),
    INDEX idx_fact_high_pop (is_high_popularity),
    INDEX idx_fact_has_grammy (has_grammy)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================================
-- 3. ANALYTICAL VIEWS FOR POWER BI DASHBOARD (Official Requirements)
-- ============================================================================

-- 3.1 REQ-01: Impact of Grammy Recognition on Popularity (By Tier and Global Lift)
CREATE OR REPLACE VIEW v_kpi_r1_grammy_vs_popularity AS
SELECT 
    CASE 
        WHEN a.has_grammy = 1 THEN 'Con Grammy'
        ELSE 'Sin Grammy'
    END AS grammy_status,
    a.nomination_tier,
    COUNT(f.fact_id) AS total_tracks,
    ROUND(AVG(f.popularity), 2) AS avg_track_popularity,
    ROUND(MAX(f.popularity), 2) AS max_track_popularity,
    ROUND(AVG(f.is_high_popularity) * 100, 2) AS high_popularity_share_pct
FROM fact_track_performance f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY 
    CASE 
        WHEN a.has_grammy = 1 THEN 'Con Grammy'
        ELSE 'Sin Grammy'
    END,
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

-- 3.2 REQ-01: Genre-Specific Popularity Lift (Highlighting Pop, R&B, Hip-Hop > +15 pts)
CREATE OR REPLACE VIEW v_kpi_r1_genre_popularity_lift AS
SELECT 
    g.genre_name,
    COUNT(f.fact_id) AS total_tracks,
    ROUND(AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity ELSE NULL END), 2) AS avg_pop_grammy,
    ROUND(AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity ELSE NULL END), 2) AS avg_pop_control,
    ROUND(
        AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity ELSE NULL END) - 
        AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity ELSE NULL END), 
        2
    ) AS popularity_lift,
    COUNT(CASE WHEN a.has_grammy = 1 THEN f.fact_id END) AS grammy_tracks_count,
    COUNT(CASE WHEN a.has_grammy = 0 THEN f.fact_id END) AS control_tracks_count
FROM fact_track_performance f
JOIN dim_genre g ON f.genre_sk = g.genre_sk
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY g.genre_name
HAVING grammy_tracks_count > 0 AND control_tracks_count > 0;

-- 3.3 REQ-02: Acoustic DNA Profile (9 Dimensions: Grammy vs Non-Grammy by Genre)
CREATE OR REPLACE VIEW v_kpi_r2_audio_features_by_grammy_genre AS
SELECT 
    g.genre_name,
    CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS grammy_status,
    COUNT(f.fact_id) AS track_count,
    ROUND(AVG(f.danceability), 4) AS avg_danceability,
    ROUND(AVG(f.energy), 4) AS avg_energy,
    ROUND(AVG(f.loudness), 2) AS avg_loudness,
    ROUND(AVG(f.speechiness), 4) AS avg_speechiness,
    ROUND(AVG(f.acousticness), 4) AS avg_acousticness,
    ROUND(AVG(f.instrumentalness), 4) AS avg_instrumentalness,
    ROUND(AVG(f.liveness), 4) AS avg_liveness,
    ROUND(AVG(f.valence), 4) AS avg_valence,
    ROUND(AVG(f.tempo), 2) AS avg_tempo
FROM fact_track_performance f
JOIN dim_genre g ON f.genre_sk = g.genre_sk
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY g.genre_name, CASE WHEN a.has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END;

-- 3.4 REQ-03: Historical Cohorts Persistence vs Control Group (HPS_P75 & CPI)
CREATE OR REPLACE VIEW v_kpi_r3_historical_cohorts_performance AS
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

