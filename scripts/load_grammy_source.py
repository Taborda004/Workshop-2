"""
Source Preparation Script: Grammy Awards Relational Source Import

Course: ETL (G01) - Universidad Autónoma de Occidente
Workshop-2: Building a Reliable Batch Data Pipeline

IMPORTANT ARCHITECTURAL DISTINCTION (PDF Sections 4.3 and 6.2):
Populating this table from the provided CSV constitutes SOURCE PREPARATION,
establishing the operational relational database required for pipeline extraction.
This is NOT the final ETL Load stage into the analytical Data Warehouse.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment configuration
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "the_grammy_awards.csv"
SQL_SETUP_PATH = BASE_DIR / "sql" / "source_setup.sql"
EVIDENCE_PATH = BASE_DIR / "docs" / "evidence" / "grammy_source_reconciliation.json"

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "1234")
SOURCE_DB = os.getenv("GRAMMY_SOURCE_DB", "grammy_source")


def run_source_preparation():
    print("=" * 70)
    print("WORKSHOP-2: GRAMMY SOURCE PREPARATION (MySQL INGESTION)")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Source CSV Path: {DATA_PATH}")
    print(f"Target Database: {SOURCE_DB} on {MYSQL_HOST}:{MYSQL_PORT}")

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Source file not found at: {DATA_PATH}")

    # Read CSV
    df = pd.read_csv(DATA_PATH)
    csv_row_count = len(df)
    print(f"[OK] Read {csv_row_count} rows from Grammy CSV.")

    # Engine for server-level connection (to create DB if needed)
    server_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/"
    engine_server = create_engine(server_url, echo=False)

    # Execute DDL setup
    print("[INFO] Executing sql/source_setup.sql...")
    with open(SQL_SETUP_PATH, "r", encoding="utf-8") as f:
        setup_sql = f.read()

    # Split and execute non-empty statements
    statements = [stmt.strip() for stmt in setup_sql.split(";") if stmt.strip()]
    with engine_server.connect() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()
    print("[OK] Source schema grammy_source and table grammy_awards_raw created.")

    # Convert winner to boolean
    df["winner"] = df["winner"].astype(bool)

    # Insert into MySQL
    db_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{SOURCE_DB}?charset=utf8mb4"
    engine_db = create_engine(db_url, echo=False)

    print("[INFO] Loading Grammy records into grammy_source.grammy_awards_raw...")
    df.to_sql(
        name="grammy_awards_raw",
        con=engine_db,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    print("[OK] Ingestion completed.")

    # Reconcile counts via SELECT COUNT(*)
    with engine_db.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM grammy_awards_raw;"))
        db_row_count = result.scalar()

    print("-" * 70)
    print(f"RECONCILIATION RESULT:")
    print(f"  CSV Source Count:  {csv_row_count}")
    print(f"  MySQL Table Count: {db_row_count}")
    print(f"  Discrepancy:       {db_row_count - csv_row_count}")
    print("-" * 70)

    is_reconciled = (csv_row_count == db_row_count)
    if not is_reconciled:
        raise ValueError(f"Reconciliation failed! CSV ({csv_row_count}) != MySQL ({db_row_count})")

    # Save evidence JSON
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    evidence_data = {
        "step": "grammy_source_preparation",
        "timestamp": datetime.now().isoformat(),
        "status": "SUCCESS",
        "source_file": str(DATA_PATH),
        "target_table": f"{SOURCE_DB}.grammy_awards_raw",
        "csv_row_count": csv_row_count,
        "mysql_row_count": db_row_count,
        "reconciliation_passed": is_reconciled,
        "verification_query": "SELECT COUNT(*) FROM grammy_source.grammy_awards_raw;",
        "architectural_note": "Source preparation only. Establishes relational source for ETL extract_grammys task."
    }

    with open(EVIDENCE_PATH, "w", encoding="utf-8") as f:
        json.dump(evidence_data, f, indent=4)

    print(f"[OK] Audit evidence saved to: {EVIDENCE_PATH}")
    print("Source preparation successful! Pipeline can now extract from MySQL.")


if __name__ == "__main__":
    run_source_preparation()
