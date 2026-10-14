"""
load.py — Data Warehouse loading for the Reliable Music Pipeline.

Loads the validated prepared dataset into the MySQL star schema (music_dw):
    dim_genre
    dim_artist  (with aggregated Grammy metrics & historical cohorts)
    fact_track  (one row per unique Track × Genre observation)

Safe-rerun strategy: Atomic DELETE + INSERT (replace entire batch).
    - Preserves true ACID transactionality using DELETE instead of TRUNCATE
      (TRUNCATE in MySQL triggers an implicit DDL commit, breaking rollback).
    - If any stage fails during load, the transaction rolls back cleanly.
    - Subsequent reruns with the same batch produce identical row counts,
      guaranteeing zero silent duplicate accumulation.
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

log = logging.getLogger(__name__)

# Dynamic Path Resolution
PROJECT_ROOT = Path(__file__).resolve().parent.parent

_raw_output_dir = os.environ.get("AIRFLOW_OUTPUT_DIR")
if _raw_output_dir:
    OUTPUT_DIR = Path(_raw_output_dir)
else:
    OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence"


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _get_dw_engine() -> sa.engine.Engine:
    """Build a SQLAlchemy engine for the music_dw MySQL Data Warehouse."""
    host     = os.environ.get("MYSQL_DW_HOST", "localhost")
    port     = os.environ.get("MYSQL_DW_PORT", "3307")
    db       = os.environ.get("MYSQL_DW_DB", "music_dw")
    user     = os.environ.get("MYSQL_DW_USER", "dw_user")
    password = os.environ.get("MYSQL_DW_PASSWORD", "dw_pass")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return sa.create_engine(url, pool_pre_ping=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS (Atomic Clean)
# ─────────────────────────────────────────────────────────────────────────────

def _clean_table(conn, table: str) -> None:
    """
    Delete all rows from a table inside the active transaction.
    Uses DELETE instead of TRUNCATE to avoid implicit DDL commits in MySQL.
    """
    conn.execute(text(f"DELETE FROM {table}"))
    log.info("Cleared table contents: %s", table)


# ─────────────────────────────────────────────────────────────────────────────
# LOAD DIMENSIONS
# ─────────────────────────────────────────────────────────────────────────────

def _load_dim_genre(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Load dim_genre and return a DataFrame with genre_name → genre_sk mapping.
    """
    genres = (
        df[["track_genre"]].drop_duplicates()
        .rename(columns={"track_genre": "genre_name"})
        .dropna()
        .reset_index(drop=True)
    )

    category_map = {
        "pop": "pop", "rock": "rock", "hip-hop": "rap", "rap": "rap",
        "r-n-b": "r&b", "soul": "r&b", "country": "country",
        "jazz": "jazz", "classical": "classical", "latin": "latin",
        "electronic": "electronic", "edm": "electronic", "dance": "electronic",
        "folk": "folk", "blues": "blues", "reggae": "reggae",
        "metal": "rock", "punk": "rock", "indie": "alternative",
        "alternative": "alternative", "ambient": "electronic",
        "acoustic": "acoustic", "gospel": "gospel",
    }

    def _family(g: str) -> str:
        g_l = g.lower()
        for kw, fam in category_map.items():
            if kw in g_l:
                return fam
        return "other"

    genres["grammy_family"] = genres["genre_name"].apply(_family)

    _clean_table(conn, "dim_genre")
    genres.to_sql("dim_genre", conn, if_exists="append", index=False,
                  method="multi", chunksize=500)

    genre_sk = pd.read_sql("SELECT genre_sk, genre_name FROM dim_genre", conn)
    log.info("dim_genre loaded: %d rows", len(genre_sk))
    print(f"[LOAD] dim_genre: {len(genre_sk)} rows")
    return genre_sk


def _load_dim_artist(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate artist-level metrics and historical cohorts from the prepared dataset.
    Returns artist_name → artist_sk mapping.
    """
    artist_agg = (
        df.groupby("primary_artist", as_index=False)
        .agg(
            has_grammy=("has_grammy", "max"),
            grammy_wins=("grammy_wins", "max"),
            grammy_nominations=("grammy_nominations", "max"),
            first_grammy_year=("first_grammy_year", "min"),
            first_grammy_decade=("first_grammy_decade", "first"),
            last_grammy_year=("last_grammy_year", "max"),
            avg_popularity=("popularity", "mean"),
            track_count=("track_id", "count"),
        )
        .rename(columns={"primary_artist": "artist_name"})
    )

    artist_agg["avg_popularity"] = artist_agg["avg_popularity"].round(2)
    artist_agg["first_grammy_decade"] = artist_agg["first_grammy_decade"].replace("Non-Grammy", None)

    for col in ["first_grammy_year", "last_grammy_year"]:
        artist_agg[col] = pd.array(artist_agg[col], dtype="Int64")

    _clean_table(conn, "dim_artist")
    artist_agg.to_sql("dim_artist", conn, if_exists="append", index=False,
                      method="multi", chunksize=500)

    artist_sk = pd.read_sql("SELECT artist_sk, artist_name FROM dim_artist", conn)
    log.info("dim_artist loaded: %d rows", len(artist_sk))
    print(f"[LOAD] dim_artist: {len(artist_sk)} rows")
    return artist_sk


# ─────────────────────────────────────────────────────────────────────────────
# LOAD FACT
# ─────────────────────────────────────────────────────────────────────────────

def _load_fact_track(conn, df: pd.DataFrame,
                     artist_sk: pd.DataFrame,
                     genre_sk: pd.DataFrame) -> int:
    """
    Map surrogate keys and insert into fact_track (Grain: Track × Genre).
    Returns the number of rows loaded.
    """
    fact = df.merge(artist_sk, left_on="primary_artist", right_on="artist_name", how="left")
    fact = fact.merge(genre_sk,  left_on="track_genre",   right_on="genre_name",   how="left")

    fact_cols = {
        "track_id":          "track_id",
        "track_name":        "track_name",
        "album_name":        "album_name",
        "artist_sk":         "artist_sk",
        "genre_sk":          "genre_sk",
        "popularity":        "popularity",
        "danceability":      "danceability",
        "energy":            "energy",
        "loudness":          "loudness",
        "speechiness":       "speechiness",
        "acousticness":      "acousticness",
        "instrumentalness":  "instrumentalness",
        "liveness":          "liveness",
        "valence":           "valence",
        "tempo":             "tempo",
        "duration_ms":       "duration_ms",
        "explicit":          "explicit",
        "key":               "musical_key",
        "mode":              "mode",
        "time_signature":    "time_signature",
        "has_grammy":        "has_grammy",
        "grammy_wins":       "grammy_wins",
        "grammy_nominations":"grammy_nominations",
    }

    fact_df = fact[list(fact_cols.keys())].rename(columns=fact_cols)

    before = len(fact_df)
    fact_df = fact_df.dropna(subset=["artist_sk", "genre_sk"])
    dropped = before - len(fact_df)
    if dropped > 0:
        log.warning("fact_track: %d rows dropped due to unresolved FKs", dropped)
        print(f"[LOAD] WARNING: {dropped} rows dropped (unresolved FKs)")

    # Round decimals
    decimal_cols = ["danceability", "energy", "loudness", "speechiness",
                    "acousticness", "instrumentalness", "liveness", "valence", "tempo"]
    for col in decimal_cols:
        fact_df[col] = fact_df[col].round(6)

    _clean_table(conn, "fact_track")
    fact_df.to_sql("fact_track", conn, if_exists="append", index=False,
                   method="multi", chunksize=1000)

    loaded = len(fact_df)
    log.info("fact_track loaded: %d rows", loaded)
    print(f"[LOAD] fact_track: {loaded} rows")
    return loaded


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def load_to_dw(prepared_path: str) -> dict:
    """
    Load the validated prepared dataset into the MySQL Data Warehouse.

    Safe-rerun strategy: Atomic DELETE + INSERT.
    Returns a load summary dict with row counts.
    """
    log.info("DW load started. Source: %s", prepared_path)
    print(f"[LOAD] Starting DW load from {prepared_path}")

    df = pd.read_parquet(prepared_path)
    source_rows = len(df)
    log.info("Prepared rows loaded: %d", source_rows)
    print(f"[LOAD] Source prepared rows: {source_rows}")

    engine = _get_dw_engine()

    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        genre_sk  = _load_dim_genre(conn, df)
        artist_sk = _load_dim_artist(conn, df)
        fact_rows = _load_fact_track(conn, df, artist_sk, genre_sk)
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))

    summary = {
        "dim_genre_rows":  len(genre_sk),
        "dim_artist_rows": len(artist_sk),
        "fact_track_rows": fact_rows,
        "source_rows":     source_rows,
    }

    log.info("DW load complete. Summary: %s", summary)
    print(f"[LOAD] Summary: {summary}")

    # Persist load evidence in both output/ and docs/evidence/
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    evidence_path = OUTPUT_DIR / "load_summary.json"
    with open(evidence_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    evidence_mirror = EVIDENCE_DIR / "load_summary.json"
    with open(evidence_mirror, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print(f"[LOAD] Evidence saved → {evidence_path}")
    return summary
