"""
load_grammy_source.py — One-time script to load the Grammy Awards CSV
into the MySQL source database.

This is SOURCE PREPARATION, not the final ETL load.
Run this ONCE before triggering the Airflow DAG.

Usage (from the project root):
    python scripts/load_grammy_source.py

The script:
1. Reads the_grammy_awards.csv from the local data directory.
2. Normalises the winner column (True/False → 1/0).
3. Inserts into grammy_source.grammy_awards_raw via SQLAlchemy.
4. Generates reconciliation evidence in docs/evidence/grammy_source_reconciliation.json.
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Grammy CSV path resolution
if (PROJECT_ROOT / "data" / "the_grammy_awards.csv").exists():
    GRAMMY_CSV = PROJECT_ROOT / "data" / "the_grammy_awards.csv"
else:
    GRAMMY_CSV = PROJECT_ROOT / "the_grammy_awards.csv"

EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence"


def get_source_engine() -> sa.engine.Engine:
    host     = os.environ.get("MYSQL_SOURCE_HOST", "localhost")
    port     = os.environ.get("MYSQL_SOURCE_PORT", "3306")
    db       = os.environ.get("MYSQL_SOURCE_DB",   "grammy_source")
    user     = os.environ.get("MYSQL_SOURCE_USER",     "etl_user")
    password = os.environ.get("MYSQL_SOURCE_PASSWORD", "etl_pass")
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return sa.create_engine(url, pool_pre_ping=True)


def load_grammy_source() -> dict:
    print(f"Reading Grammy CSV: {GRAMMY_CSV}")

    if not GRAMMY_CSV.exists():
        print(f"ERROR: File not found: {GRAMMY_CSV}")
        sys.exit(1)

    df = pd.read_csv(GRAMMY_CSV, low_memory=False)
    csv_rows = len(df)
    print(f"CSV rows read: {csv_rows}")

    # Normalise columns
    df["winner"] = df["winner"].map(
        {True: 1, False: 0, "True": 1, "False": 0}
    ).fillna(0).astype(int)

    insert_cols = [
        "year", "title", "published_at", "updated_at",
        "category", "nominee", "artist", "workers", "img", "winner",
    ]
    df = df[[c for c in insert_cols if c in df.columns]]

    engine = get_source_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE grammy_awards_raw"))
        print("Existing rows cleared (TRUNCATE).")

        df.to_sql(
            "grammy_awards_raw",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )

    # Reconciliation check
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) AS n, SUM(winner) AS wins FROM grammy_awards_raw")
        ).fetchone()
        db_rows = result.n
        wins = result.wins
        match = bool(db_rows == csv_rows)
        print(f"Source DB rows: {db_rows} | Winners: {wins} | Match: {match}")

    summary = {
        "source_file": str(GRAMMY_CSV.name),
        "csv_rows_read": csv_rows,
        "db_rows_inserted": db_rows,
        "total_winners": int(wins),
        "reconciliation_match": match,
        "temporal_range": {
            "min_year": int(df["year"].min()),
            "max_year": int(df["year"].max()),
        },
        "target_table": "grammy_source.grammy_awards_raw",
        "status": "SUCCESS" if match else "FAILED_RECONCILIATION",
    }

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / "grammy_source_reconciliation.json"
    with open(evidence_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print(f"Grammy source load reconciliation evidence written → {evidence_path}")
    return summary


if __name__ == "__main__":
    load_grammy_source()
