"""
extract.py — Extraction functions for the Reliable Music Pipeline.

Two independent source branches:
    Branch A: Spotify CSV  → extract_spotify()
    Branch B: Grammy MySQL → extract_grammys()

Both functions:
    - Read ONLY from their assigned source.
    - Write a raw working file under WORK_DIR as Apache Parquet.
    - Return a file path string (never a DataFrame).
    - Log source, row count, schema, and output path.
"""

import os
import logging
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

log = logging.getLogger(__name__)

# ── Dynamic Path Resolution (Supports Docker & Local Environments) ────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

_raw_data_dir = os.environ.get("AIRFLOW_WORK_DIR")
if _raw_data_dir:
    WORK_DIR = Path(_raw_data_dir)
else:
    WORK_DIR = PROJECT_ROOT / "data" / "work"

_raw_output_dir = os.environ.get("AIRFLOW_OUTPUT_DIR")
if _raw_output_dir:
    OUTPUT_DIR = Path(_raw_output_dir)
else:
    OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

# Spotify Source Path resolution (checks env var, then data/, then root)
_env_spotify = os.environ.get("SPOTIFY_CSV_PATH")
if _env_spotify and Path(_env_spotify).exists():
    SPOTIFY_CSV_PATH = Path(_env_spotify)
elif (PROJECT_ROOT / "data" / "spotify_dataset.csv").exists():
    SPOTIFY_CSV_PATH = PROJECT_ROOT / "data" / "spotify_dataset.csv"
else:
    SPOTIFY_CSV_PATH = PROJECT_ROOT / "spotify_dataset.csv"

# Raw output paths
SPOTIFY_RAW_PATH = WORK_DIR / "spotify_raw.parquet"
GRAMMY_RAW_PATH  = WORK_DIR / "grammy_raw.parquet"

# ── Columns required by the analytical requirements ───────────────────────────
SPOTIFY_REQUIRED_COLS = [
    "track_id", "artists", "album_name", "track_name",
    "popularity", "duration_ms", "explicit",
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature",
    "track_genre",
]

GRAMMY_REQUIRED_COLS = [
    "year", "category", "nominee", "artist", "winner",
]


# ─────────────────────────────────────────────────────────────────────────────
# BRANCH A — Spotify CSV
# ─────────────────────────────────────────────────────────────────────────────

def extract_spotify() -> str:
    """
    Read the Spotify CSV from the mounted volume / data directory and
    persist a raw Parquet working file.

    Returns:
        Absolute path to the raw Parquet file (str).

    Raises:
        FileNotFoundError: if the source CSV is missing.
        ValueError: if any required column is absent.
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Spotify extraction started. Source path: %s", SPOTIFY_CSV_PATH)

    if not SPOTIFY_CSV_PATH.exists():
        raise FileNotFoundError(
            f"Spotify CSV not found at {SPOTIFY_CSV_PATH}. "
            "Ensure the data directory is correctly mounted."
        )

    df = pd.read_csv(SPOTIFY_CSV_PATH, low_memory=False)

    # Drop unnamed index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Column presence check
    missing = [c for c in SPOTIFY_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Spotify source is missing required columns: {missing}")

    raw_df = df[SPOTIFY_REQUIRED_COLS].copy()
    source_rows = len(raw_df)

    raw_df.to_parquet(SPOTIFY_RAW_PATH, index=False)

    log.info(
        "Spotify extraction complete. "
        "Source rows: %d | Columns: %d | Raw file: %s",
        source_rows, len(raw_df.columns), SPOTIFY_RAW_PATH,
    )

    print(f"[EXTRACT] Spotify | source_rows={source_rows} | raw_path={SPOTIFY_RAW_PATH}")
    return str(SPOTIFY_RAW_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# BRANCH B — Grammy Awards MySQL
# ─────────────────────────────────────────────────────────────────────────────

def _get_source_engine() -> sa.engine.Engine:
    """Build a SQLAlchemy engine for the Grammy source MySQL database."""
    host     = os.environ.get("MYSQL_SOURCE_HOST", "localhost")
    port     = os.environ.get("MYSQL_SOURCE_PORT", "3306")
    db       = os.environ.get("MYSQL_SOURCE_DB", "grammy_source")
    user     = os.environ.get("MYSQL_SOURCE_USER", "etl_user")
    password = os.environ.get("MYSQL_SOURCE_PASSWORD", "etl_pass")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return sa.create_engine(url, pool_pre_ping=True)


def extract_grammys() -> str:
    """
    Query the Grammy Awards relational source database and persist a raw
    Parquet working file.

    The Grammy source DB must already be populated (see source_setup.sql
    and scripts/load_grammy_source.py).

    Returns:
        Absolute path to the raw Parquet file (str).

    Raises:
        sqlalchemy.exc.OperationalError: if the DB is unreachable.
        ValueError: if the source table is empty or missing columns.
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Grammy extraction started from MySQL source DB.")

    engine = _get_source_engine()

    query = """
        SELECT
            id,
            year,
            title,
            category,
            nominee,
            artist,
            winner,
            loaded_at
        FROM grammy_awards_raw
        ORDER BY year, id
    """

    with engine.connect() as conn:
        df = pd.read_sql(sa.text(query), conn)

    if df.empty:
        raise ValueError(
            "Grammy source table 'grammy_awards_raw' is empty. "
            "Run scripts/load_grammy_source.py first."
        )

    # Column presence check
    missing = [c for c in GRAMMY_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Grammy source is missing required columns: {missing}")

    source_rows = len(df)
    df.to_parquet(GRAMMY_RAW_PATH, index=False)

    log.info(
        "Grammy extraction complete. "
        "Source rows: %d | Raw file: %s",
        source_rows, GRAMMY_RAW_PATH,
    )

    print(f"[EXTRACT] Grammy | source_rows={source_rows} | raw_path={GRAMMY_RAW_PATH}")
    return str(GRAMMY_RAW_PATH)
