-- ============================================================================
-- WORKSHOP-2: DIMENSIONAL DATA WAREHOUSE SCHEMA (MySQL 8.0)
-- Target Database: spotify_grammy_dw
-- Purpose: Kimball Star Schema supporting REQ-01, REQ-02, REQ-03
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

