"""
Data Warehouse Loading Module: Workshop-2 ETL Pipeline.

Implements transactional ACID loading into the MySQL Star Schema (spotify_grammy_dw).
Guarantees repeatability and safe reruns through atomic reverse-referential cleanups
and bulk loading inside an atomic SQLAlchemy transaction block (engine.begin()).
"""

import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
EVIDENCE_DIR = BASE_DIR / "docs" / "evidence"


def load_data_warehouse(prepared_parquet_path: str) -> dict:
    """
    Perform an atomic, transactional load of the validated prepared dataset
    into spotify_grammy_dw.

    Returns:
        dict: Load summary containing row counts, timestamps, and integrity status.
    """
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("WORKSHOP-2: TRANSACTIONAL DATA WAREHOUSE LOAD (MySQL)")
    print("=" * 70)

    prepared_path = Path(prepared_parquet_path)
    if not prepared_path.exists():
        raise FileNotFoundError(f"Prepared dataset not found at: {prepared_path}")

    df = pd.read_parquet(prepared_path)
    total_records = len(df)
    print(f"[LOAD DW] Ingesting {total_records:,} prepared records from {prepared_path.name}")

    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_port = int(os.getenv("MYSQL_PORT", 3306))
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "1234")
    dw_db = os.getenv("DW_DATABASE", "spotify_grammy_dw")

    dw_url = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{dw_db}?charset=utf8mb4"
    engine = create_engine(dw_url, echo=False)

    # 1. Prepare Date Dimension Data
    years = list(range(1950, 2031))
    date_rows = []
    for y in years:
        date_rows.append({
            "date_sk": y,
            "award_year": y,
            "decade": f"{(y // 10) * 10}s",
            "is_grammy_era": 1 if y <= 2026 else 0
        })
    # Add sentinel for tracks without Grammy year
    date_rows.append({
        "date_sk": 9999,
        "award_year": 9999,
        "decade": "Sin Grammy",
        "is_grammy_era": 0
    })
    df_date = pd.DataFrame(date_rows)

    # 2. Prepare Genre Dimension Data
    genres = sorted(df["track_genre"].dropna().unique().tolist())
    df_genre = pd.DataFrame({"genre_name": genres})

    # 3. Prepare Artist Dimension Data
    # Deduplicate artists to unique primary_artist
    df_artist_unique = df.groupby("primary_artist", as_index=False).agg(
        has_grammy=("has_grammy", "max"),
        total_nominations=("total_nominations", "max"),
        total_wins=("total_wins", "max"),
        nomination_tier=("nomination_tier", "first"),
        first_grammy_year=("first_grammy_year", "first"),
        first_grammy_decade=("first_grammy_decade", "first")
    ).rename(columns={"primary_artist": "artist_name"})

    print(f"[LOAD DW] Prepared dimensions: {len(df_genre)} genres, {len(df_artist_unique):,} artists, {len(df_date)} dates.")

    # 4. Atomic Transaction Execution
    with engine.begin() as conn:
        print("[LOAD DW] Beginning atomic transaction. Executing controlled reverse referential cleanup...")
        # Safe cleanup in reverse dependency order (avoids MySQL TRUNCATE implicit commit)
        conn.execute(text("DELETE FROM fact_track_performance;"))
        conn.execute(text("DELETE FROM dim_artist;"))
        conn.execute(text("DELETE FROM dim_genre;"))
        conn.execute(text("DELETE FROM dim_date;"))

        # Load Date Dimension
        df_date.to_sql("dim_date", con=conn, if_exists="append", index=False)

        # Load Genre Dimension
        df_genre.to_sql("dim_genre", con=conn, if_exists="append", index=False)

        # Load Artist Dimension
        df_artist_unique.to_sql("dim_artist", con=conn, if_exists="append", index=False, chunksize=1000)

        # Retrieve Generated Surrogate Keys
        genre_map = pd.read_sql("SELECT genre_sk, genre_name FROM dim_genre", conn).set_index("genre_name")["genre_sk"].to_dict()
        artist_map = pd.read_sql("SELECT artist_sk, artist_name FROM dim_artist", conn).set_index("artist_name")["artist_sk"].to_dict()

        # Map Keys to Fact Table
        df_fact = pd.DataFrame()
        df_fact["track_id"] = df["track_id"]
        df_fact["artist_sk"] = df["primary_artist"].map(artist_map)
        df_fact["genre_sk"] = df["track_genre"].map(genre_map)
        df_fact["date_sk"] = df["first_grammy_year"].fillna(9999).astype(int)

        df_fact["track_name"] = df["track_name"]
        df_fact["album_name"] = df["album_name"]
        df_fact["popularity"] = df["popularity"].astype(int)
        df_fact["is_high_popularity"] = df["is_high_popularity"].astype(int)
        df_fact["duration_ms"] = df["duration_ms"].astype(int)
        df_fact["explicit"] = df["explicit"].astype(int)

        # 9 Audio Dimensions
        df_fact["danceability"] = df["danceability"]
        df_fact["energy"] = df["energy"]
        df_fact["key_signature"] = df["key"].astype(int)
        df_fact["loudness"] = df["loudness"]
        df_fact["audio_mode"] = df["mode"].astype(int)
        df_fact["speechiness"] = df["speechiness"]
        df_fact["acousticness"] = df["acousticness"]
        df_fact["instrumentalness"] = df["instrumentalness"]
        df_fact["liveness"] = df["liveness"]
        df_fact["valence"] = df["valence"]
        df_fact["tempo"] = df["tempo"]
        df_fact["time_signature"] = df["time_signature"].astype(int)
        df_fact["has_grammy"] = df["has_grammy"].astype(int)

        print("[LOAD DW] Inserting fact records into fact_track_performance...")
        df_fact.to_sql("fact_track_performance", con=conn, if_exists="append", index=False, chunksize=5000)

        # Immediate verification inside the transaction
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_track_performance;")).scalar()
        artist_count = conn.execute(text("SELECT COUNT(*) FROM dim_artist;")).scalar()
        genre_count = conn.execute(text("SELECT COUNT(*) FROM dim_genre;")).scalar()
        date_count = conn.execute(text("SELECT COUNT(*) FROM dim_date;")).scalar()

    print("-" * 70)
    print("DATA WAREHOUSE LOAD SUMMARY:")
    print(f"  Target Database:           {dw_db}")
    print(f"  fact_track_performance:    {fact_count:,} rows (Expected: 113,549)")
    print(f"  dim_artist:                {artist_count:,} rows")
    print(f"  dim_genre:                 {genre_count:,} rows")
    print(f"  dim_date:                  {date_count:,} rows")
    print("-" * 70)

    if fact_count != total_records:
        raise ValueError(f"Load reconciliation mismatch: expected {total_records}, loaded {fact_count}")

    summary = {
        "step": "load_data_warehouse",
        "timestamp": datetime.now().isoformat(),
        "status": "SUCCESS",
        "database": dw_db,
        "tables": {
            "fact_track_performance": fact_count,
            "dim_artist": artist_count,
            "dim_genre": genre_count,
            "dim_date": date_count
        },
        "safe_rerun_strategy": "Atomic transaction with engine.begin() and reverse-referential DELETE FROM.",
        "reconciliation_passed": (fact_count == 113549)
    }

    evidence_path = EVIDENCE_DIR / "dw_load_summary.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4)

    print(f"[OK] Audit summary saved to: {evidence_path}")
    return summary
