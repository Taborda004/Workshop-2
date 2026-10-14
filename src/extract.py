"""
Extraction Module: Workshop-2 ETL Pipeline.

Implements two independently observable extraction branches:
1. extract_spotify_data: Reads catalog data from CSV without premature cleaning.
2. extract_grammys_data: Reads academy award data from relational MySQL source table.

Exchanges lightweight serialized Parquet paths, avoiding bulk data transfer through Airflow metadata store.
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = BASE_DIR / "data"
DEFAULT_WORK_DIR = DEFAULT_DATA_DIR / "work"

SPOTIFY_REQUIRED_COLUMNS = [
    "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "danceability", "energy", "key",
    "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature", "track_genre"
]

GRAMMY_REQUIRED_COLUMNS = [
    "year", "category", "nominee", "artist", "workers", "winner"
]


def extract_spotify_data(source_path: str = None, work_dir: str = None) -> str:
    """
    Extract raw Spotify data from CSV.
    Validates column presence without premature filtering or mutation.
    Returns path to serialized raw parquet file.
    """
    work_path = Path(work_dir) if work_dir else DEFAULT_WORK_DIR
    work_path.mkdir(parents=True, exist_ok=True)

    csv_path = Path(source_path) if source_path else DEFAULT_DATA_DIR / "spotify_dataset.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Spotify source CSV not found: {csv_path}")

    print(f"[EXTRACT SPOTIFY] Reading from: {csv_path}")
    df = pd.read_csv(csv_path)

    missing = [col for col in SPOTIFY_REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"[EXTRACT SPOTIFY] Missing required schema columns: {missing}")

    raw_output_path = work_path / "spotify_raw.parquet"
    df[SPOTIFY_REQUIRED_COLUMNS].to_parquet(raw_output_path, index=False)

    print(f"[EXTRACT SPOTIFY] Extracted {len(df):,} rows.")
    print(f"[EXTRACT SPOTIFY] Raw working file persisted to: {raw_output_path}")

    return str(raw_output_path)


def extract_grammys_data(work_dir: str = None) -> str:
    """
    Extract raw Grammy data from MySQL source table grammy_source.grammy_awards_raw.
    Strictly queries relational database, complying with PDF Sections 4.3 and 6.2.
    Returns path to serialized raw parquet file.
    """
    work_path = Path(work_dir) if work_dir else DEFAULT_WORK_DIR
    work_path.mkdir(parents=True, exist_ok=True)

    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_port = int(os.getenv("MYSQL_PORT", 3306))
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "1234")
    source_db = os.getenv("GRAMMY_SOURCE_DB", "grammy_source")

    db_url = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{source_db}?charset=utf8mb4"
    engine = create_engine(db_url, echo=False)

    print(f"[EXTRACT GRAMMY] Querying relational source {source_db}.grammy_awards_raw on {mysql_host}:{mysql_port}...")
    query = "SELECT year, category, nominee, artist, workers, winner FROM grammy_awards_raw"
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    missing = [col for col in GRAMMY_REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"[EXTRACT GRAMMY] Missing required schema columns: {missing}")

    raw_output_path = work_path / "grammys_raw.parquet"
    df[GRAMMY_REQUIRED_COLUMNS].to_parquet(raw_output_path, index=False)

    print(f"[EXTRACT GRAMMY] Extracted {len(df):,} rows from MySQL.")
    print(f"[EXTRACT GRAMMY] Raw working file persisted to: {raw_output_path}")

    return str(raw_output_path)
