-- ============================================================
-- Data Warehouse Star Schema (Aligned with Phase 0 & PDF)
-- Workshop 2 - ETL Course
--
-- Business Process: Music Streaming Performance and Grammy Recognition
-- Grain: One row per unique Spotify Track Observation by Genre (Track × Genre)
--
-- Analytical Requirements Supported:
--   REQ-01: Grammy Recognition vs. Spotify Popularity (Popularity Lift by Genre & Nominations)
--   REQ-02: Grammy Artists' Sonic Profile (9 Musical Features by Genre)
--   REQ-03: Historical Grammy Cohorts & Current Streaming Performance (CPI, HPS, P75 by Decade)
-- ============================================================

CREATE DATABASE IF NOT EXISTS music_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE music_dw;

-- ════════════════════════════════════════════════════════════
-- DROP EXISTING TABLES IN REVERSE DEPENDENCY ORDER
-- ════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS fact_track;
DROP TABLE IF EXISTS dim_artist;
DROP TABLE IF EXISTS dim_genre;
DROP TABLE IF EXISTS dim_date;

-- ════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ════════════════════════════════════════════════════════════

-- ── dim_artist ────────────────────────────────────────────────────────────────
-- Grain: One row per unique primary artist.
-- Enriched with aggregated Grammy recognition metrics and historical cohorts.
CREATE TABLE dim_artist (
    artist_sk            INT AUTO_INCREMENT PRIMARY KEY  COMMENT 'Surrogate key',
    artist_name          VARCHAR(500) NOT NULL           COMMENT 'Primary artist name (business key)',

    -- Grammy enrichment & historical cohort attributes (REQ-01, REQ-03)
    has_grammy           TINYINT(1)   NOT NULL DEFAULT 0  COMMENT '1 = recognized with at least one Grammy nomination/win',
    grammy_wins          INT          NOT NULL DEFAULT 0  COMMENT 'Total Grammy wins across all ceremonies',
    grammy_nominations   INT          NOT NULL DEFAULT 0  COMMENT 'Total Grammy nominations across all ceremonies',
    first_grammy_year    INT          NULL                COMMENT 'Earliest award year (debut recognition)',
    first_grammy_decade  VARCHAR(10)  NULL                COMMENT 'Historical Cohort: 1960s, 1970s, 1980s, 1990s, 2000s, 2010s',
    last_grammy_year     INT          NULL                COMMENT 'Most recent award year',

    -- Streaming aggregates
    avg_popularity       DECIMAL(5,2) NULL                COMMENT 'Artist average track popularity (0-100)',
    track_count          INT          NOT NULL DEFAULT 0  COMMENT 'Total catalog tracks observed in Spotify',

    dw_loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_artist_name (artist_name(255))
);

-- ── dim_genre ─────────────────────────────────────────────────────────────────
-- Grain: One row per unique Spotify track_genre.
CREATE TABLE dim_genre (
    genre_sk      INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    genre_name    VARCHAR(100) NOT NULL          COMMENT 'Spotify genre label (business key)',
    grammy_family VARCHAR(100) NULL              COMMENT 'Broad genre family (pop, rock, r&b, rap, latin, jazz, etc.)',

    dw_loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_genre_name (genre_name)
);

-- ── dim_date ──────────────────────────────────────────────────────────────────
-- Grain: One row per calendar year (Grammy timeline 1958–2030).
CREATE TABLE dim_date (
    date_sk  INT PRIMARY KEY      COMMENT 'Year key (YYYY)',
    year     INT NOT NULL         COMMENT 'Calendar year',
    decade   VARCHAR(10) NOT NULL COMMENT 'Decade label (e.g., 1960s, 2010s)'
);

-- Pre-populate years (1958–2030)
INSERT INTO dim_date (date_sk, year, decade)
SELECT y,
       y,
       CONCAT(FLOOR(y / 10) * 10, 's')
FROM (
    WITH RECURSIVE seq AS (
        SELECT 1958 AS y
        UNION ALL
        SELECT y + 1 FROM seq WHERE y < 2030
    )
    SELECT y FROM seq
) sub;

-- ════════════════════════════════════════════════════════════
-- FACT TABLE
-- ════════════════════════════════════════════════════════════

-- ── fact_track ────────────────────────────────────────────────────────────────
-- Grain: One observation per Track × Genre (reflects multi-genre streaming presence).
CREATE TABLE fact_track (
    track_sk              INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    track_id              VARCHAR(50)  NOT NULL          COMMENT 'Spotify track identifier',
    track_name            VARCHAR(500) NULL,
    album_name            VARCHAR(500) NULL,

    -- Foreign keys
    artist_sk             INT NOT NULL,
    genre_sk              INT NOT NULL,

    -- Popularity measures (REQ-01, REQ-03)
    popularity            INT NOT NULL DEFAULT 0        COMMENT 'Spotify popularity metric (0–100)',

    -- Musical audio features (REQ-02)
    danceability          DECIMAL(5,4) NULL             COMMENT 'Danceability index (0.0 to 1.0)',
    energy                DECIMAL(5,4) NULL             COMMENT 'Perceptual energy index (0.0 to 1.0)',
    loudness              DECIMAL(7,3) NULL             COMMENT 'Overall loudness in decibels (dB)',
    speechiness           DECIMAL(5,4) NULL             COMMENT 'Presence of spoken words (0.0 to 1.0)',
    acousticness          DECIMAL(5,4) NULL             COMMENT 'Acoustic confidence index (0.0 to 1.0)',
    instrumentalness      DECIMAL(5,4) NULL             COMMENT 'Instrumentalness score (0.0 to 1.0)',
    liveness              DECIMAL(5,4) NULL             COMMENT 'Audience presence probability (0.0 to 1.0)',
    valence               DECIMAL(5,4) NULL             COMMENT 'Musical positiveness/mood (0.0 to 1.0)',
    tempo                 DECIMAL(7,3) NULL             COMMENT 'Estimated tempo in beats per minute (BPM)',
    duration_ms           INT          NULL             COMMENT 'Track length in milliseconds',
    explicit              TINYINT(1)   NOT NULL DEFAULT 0,
    musical_key           TINYINT      NULL             COMMENT 'Pitch class (0 to 11)',
    mode                  TINYINT      NULL             COMMENT 'Modality (0 = minor, 1 = major)',
    time_signature        TINYINT      NULL             COMMENT 'Estimated meter signature',

    -- Denormalised Grammy metrics for performant analytical querying
    has_grammy            TINYINT(1)   NOT NULL DEFAULT 0 COMMENT 'Grammy recognition indicator',
    grammy_wins           INT          NOT NULL DEFAULT 0 COMMENT 'Artist total wins',
    grammy_nominations    INT          NOT NULL DEFAULT 0 COMMENT 'Artist total nominations',

    dw_loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_artist FOREIGN KEY (artist_sk) REFERENCES dim_artist (artist_sk),
    CONSTRAINT fk_fact_genre  FOREIGN KEY (genre_sk)  REFERENCES dim_genre  (genre_sk),

    -- Grain enforcement: Exactly one row per (track_id, genre_sk)
    UNIQUE KEY uq_track_genre (track_id, genre_sk)
);

-- ── Performance indexes ───────────────────────────────────────────────────────
CREATE INDEX idx_fact_artist     ON fact_track (artist_sk);
CREATE INDEX idx_fact_genre      ON fact_track (genre_sk);
CREATE INDEX idx_fact_has_grammy ON fact_track (has_grammy);
CREATE INDEX idx_fact_popularity ON fact_track (popularity);

-- ════════════════════════════════════════════════════════════
-- ANALYTICAL KPI VIEWS (Direct Target for Power BI & Queries)
-- ════════════════════════════════════════════════════════════

-- ── VIEW 1: REQ-01 — Grammy vs Popularity & Nomination Buckets ───────────────
CREATE OR REPLACE VIEW v_kpi_r1_grammy_vs_popularity AS
SELECT
    g.genre_name,
    a.has_grammy,
    CASE
        WHEN a.has_grammy = 0 THEN 'Non-Grammy'
        ELSE 'Grammy Recognized'
    END AS recognition_group,
    CASE
        WHEN a.grammy_nominations = 0 THEN '0 nominations'
        WHEN a.grammy_nominations BETWEEN 1 AND 2 THEN '1-2 nominations'
        WHEN a.grammy_nominations BETWEEN 3 AND 5 THEN '3-5 nominations'
        ELSE '6+ nominations'
    END AS nomination_bucket,
    COUNT(DISTINCT a.artist_sk)         AS artist_count,
    COUNT(f.track_sk)                   AS total_tracks,
    ROUND(AVG(f.popularity), 2)         AS avg_popularity,
    ROUND(STDDEV(f.popularity), 2)      AS std_popularity,
    ROUND(MIN(f.popularity), 2)         AS min_popularity,
    ROUND(MAX(f.popularity), 2)         AS max_popularity,
    ROUND(AVG(a.grammy_nominations), 2) AS avg_nominations,
    ROUND(AVG(a.grammy_wins), 2)        AS avg_wins
FROM fact_track f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
JOIN dim_genre  g ON f.genre_sk  = g.genre_sk
GROUP BY g.genre_name, a.has_grammy, nomination_bucket;

-- ── VIEW 2: REQ-01 Advanced — Popularity Lift by Genre ───────────────────────
CREATE OR REPLACE VIEW v_kpi_r1_genre_popularity_lift AS
SELECT
    g.genre_name,
    COUNT(DISTINCT CASE WHEN a.has_grammy = 1 THEN a.artist_sk END) AS grammy_artists,
    COUNT(DISTINCT CASE WHEN a.has_grammy = 0 THEN a.artist_sk END) AS nongrammy_artists,
    COUNT(CASE WHEN a.has_grammy = 1 THEN f.track_sk END)           AS grammy_tracks,
    COUNT(CASE WHEN a.has_grammy = 0 THEN f.track_sk END)           AS nongrammy_tracks,
    ROUND(AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity END), 2) AS avg_pop_grammy,
    ROUND(AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity END), 2) AS avg_pop_nongrammy,
    ROUND(
        AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity END) -
        AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity END),
        2
    ) AS absolute_difference,
    ROUND(
        (
            AVG(CASE WHEN a.has_grammy = 1 THEN f.popularity END) -
            AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity END)
        ) / NULLIF(AVG(CASE WHEN a.has_grammy = 0 THEN f.popularity END), 0) * 100,
        2
    ) AS popularity_lift_pct
FROM fact_track f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
JOIN dim_genre  g ON f.genre_sk  = g.genre_sk
GROUP BY g.genre_name
HAVING grammy_tracks > 0 AND nongrammy_tracks > 0;

-- ── VIEW 3: REQ-02 — Sonic Profile of Grammy vs Non-Grammy by Genre ─────────
CREATE OR REPLACE VIEW v_kpi_r2_audio_features_by_grammy_genre AS
SELECT
    g.genre_name,
    a.has_grammy,
    CASE WHEN a.has_grammy = 1 THEN 'Grammy Artist' ELSE 'Non-Grammy Artist' END AS artist_group,
    COUNT(f.track_sk)                  AS track_count,
    ROUND(AVG(f.danceability), 4)      AS avg_danceability,
    ROUND(AVG(f.energy), 4)            AS avg_energy,
    ROUND(AVG(f.loudness), 3)          AS avg_loudness,
    ROUND(AVG(f.speechiness), 4)       AS avg_speechiness,
    ROUND(AVG(f.acousticness), 4)      AS avg_acousticness,
    ROUND(AVG(f.instrumentalness), 4)  AS avg_instrumentalness,
    ROUND(AVG(f.liveness), 4)          AS avg_liveness,
    ROUND(AVG(f.valence), 4)           AS avg_valence,
    ROUND(AVG(f.tempo), 2)             AS avg_tempo,
    ROUND(AVG(f.popularity), 2)        AS avg_popularity
FROM fact_track f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
JOIN dim_genre  g ON f.genre_sk  = g.genre_sk
GROUP BY g.genre_name, a.has_grammy;

-- ── VIEW 4: REQ-03 — Historical Grammy Cohorts & Current Streaming (CPI, HPS) ─
-- Threshold P75 = 50.0 (empirically derived 75th percentile of Spotify popularity)
CREATE OR REPLACE VIEW v_kpi_r3_historical_cohorts_performance AS
SELECT
    COALESCE(a.first_grammy_decade, 'Non-Grammy') AS historical_cohort,
    CASE WHEN a.has_grammy = 1 THEN 'Grammy Recognized' ELSE 'Control Group (Non-Grammy)' END AS artist_cohort_type,
    COUNT(DISTINCT a.artist_sk)                   AS artist_count,
    COUNT(f.track_sk)                             AS track_count,
    ROUND(AVG(f.popularity), 2)                   AS avg_popularity,
    ROUND(STDDEV(f.popularity), 2)                AS std_popularity,
    50.0                                          AS p75_threshold,
    -- HPS: High Popularity Share (% of tracks with popularity >= P75)
    ROUND(
        SUM(CASE WHEN f.popularity >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS hps_percentage,
    -- CPI: Career / Cohort Performance Index: weighted composite of volume & popularity
    ROUND(
        (AVG(f.popularity) * 0.5) +
        ((SUM(CASE WHEN f.popularity >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) * 0.5),
        2
    ) AS cpi_index,
    ROUND(AVG(a.grammy_wins), 2)        AS avg_wins,
    ROUND(AVG(a.grammy_nominations), 2) AS avg_nominations
FROM fact_track f
JOIN dim_artist a ON f.artist_sk = a.artist_sk
GROUP BY a.first_grammy_decade, a.has_grammy;

-- ── VIEW 5: Artist-Level Summary for Power BI Slicers & Cross-Filtering ───────
CREATE OR REPLACE VIEW v_kpi_artist_summary AS
SELECT
    a.artist_name,
    a.has_grammy,
    a.first_grammy_year,
    a.first_grammy_decade,
    a.last_grammy_year,
    a.grammy_wins,
    a.grammy_nominations,
    a.avg_popularity,
    a.track_count,
    -- High Popularity Share for this specific artist
    ROUND(
        SUM(CASE WHEN f.popularity >= 50 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(f.track_sk), 0),
        2
    ) AS artist_hps_pct
FROM dim_artist a
LEFT JOIN fact_track f ON a.artist_sk = f.artist_sk
GROUP BY
    a.artist_sk, a.artist_name, a.has_grammy, a.first_grammy_year,
    a.first_grammy_decade, a.last_grammy_year, a.grammy_wins,
    a.grammy_nominations, a.avg_popularity, a.track_count;
