-- ============================================================
-- Grammy Awards Source Database Setup
-- Workshop 2 - ETL Course
--
-- PURPOSE: Load the Grammy Awards CSV into a relational source
-- table. This is SOURCE PREPARATION, not the final ETL load.
-- The pipeline will EXTRACT from this table, not from the CSV.
-- ============================================================

CREATE DATABASE IF NOT EXISTS grammy_source
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE grammy_source;

-- ── Grammy Awards source table ────────────────────────────────────────────────
DROP TABLE IF EXISTS grammy_awards_raw;

CREATE TABLE grammy_awards_raw (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    year          INT            NOT NULL COMMENT 'Grammy ceremony year',
    title         VARCHAR(255)   NOT NULL COMMENT 'Ceremony title',
    published_at  VARCHAR(50)    NULL COMMENT 'ISO timestamp published',
    updated_at    VARCHAR(50)    NULL COMMENT 'ISO timestamp updated',
    category      VARCHAR(300)   NOT NULL COMMENT 'Award category name',
    nominee       VARCHAR(500)   NULL COMMENT 'Nominated work (song/album)',
    artist        VARCHAR(500)   NULL COMMENT 'Nominated artist name',
    workers       TEXT           NULL COMMENT 'Producers, engineers, etc.',
    img           VARCHAR(1000)  NULL COMMENT 'Artist image URL',
    winner        TINYINT(1)     NOT NULL DEFAULT 0 COMMENT '1=winner, 0=nominee',
    loaded_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL load timestamp'
);

-- ── Import from CSV (LOAD DATA LOCAL) ────────────────────────────────────────
-- NOTE: The CSV is mounted at /data/the_grammy_awards.csv in the container.
-- Run this manually ONCE after the container starts, or use the
-- scripts/load_grammy_source.py helper script.
--
-- LOAD DATA LOCAL INFILE '/data/the_grammy_awards.csv'
-- INTO TABLE grammy_awards_raw
-- FIELDS TERMINATED BY ','
-- OPTIONALLY ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (year, title, published_at, updated_at, category, nominee, artist,
--  workers, img, @winner_val)
-- SET winner = IF(@winner_val = 'True', 1, 0);

-- ── Indexes for ETL extraction performance ────────────────────────────────────
CREATE INDEX idx_grammy_artist  ON grammy_awards_raw (artist(100));
CREATE INDEX idx_grammy_year    ON grammy_awards_raw (year);
CREATE INDEX idx_grammy_winner  ON grammy_awards_raw (winner);
CREATE INDEX idx_grammy_cat     ON grammy_awards_raw (category(100));

-- ── Verification query (run after load) ──────────────────────────────────────
-- SELECT COUNT(*) AS total_rows,
--        SUM(winner) AS total_winners,
--        MIN(year) AS min_year,
--        MAX(year) AS max_year
-- FROM grammy_awards_raw;
