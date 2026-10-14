-- ============================================================================
-- WORKSHOP-2: SOURCE PREPARATION SCRIPT
-- Purpose: Setup operational source database and table for Grammy Awards.
--
-- IMPORTANT ARCHITECTURAL NOTE:
-- Populating this table from the provided CSV constitutes SOURCE PREPARATION,
-- establishing the operational relational database required for extraction.
-- This is NOT the final ETL Load stage into the analytical Data Warehouse.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS grammy_source
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE grammy_source;

DROP TABLE IF EXISTS grammy_awards_raw;

CREATE TABLE grammy_awards_raw (
    source_row_id INT AUTO_INCREMENT PRIMARY KEY,
    year INT NOT NULL,
    title VARCHAR(500) NULL,
    published_at VARCHAR(100) NULL,
    updated_at VARCHAR(100) NULL,
    category VARCHAR(500) NULL,
    nominee VARCHAR(500) NULL,
    artist VARCHAR(500) NULL,
    workers TEXT NULL,
    img TEXT NULL,
    winner BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_grammy_year (year),
    INDEX idx_grammy_artist (artist),
    INDEX idx_grammy_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
