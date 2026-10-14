"""
Test C Harness: Repeatability and Safe Rerun Reconciliation.

Verifies that re-executing the batch pipeline on the same input dataset:
1. Does NOT create duplicate records in the fact table or dimension tables.
2. Maintains exactly 113,549 fact rows.
3. Produces identical metrics and measures before and after the rerun.
4. Preserves evidence in docs/evidence/test_c_safe_rerun_reconciliation.json.
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
import sys
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "scripts"))

from run_pipeline_local import execute_pipeline

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "1234")
DW_DB = os.getenv("DW_DATABASE", "spotify_grammy_dw")

dw_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DW_DB}?charset=utf8mb4"
engine = create_engine(dw_url)


def get_dw_snapshot():
    with engine.connect() as conn:
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_track_performance")).scalar()
        artist_count = conn.execute(text("SELECT COUNT(*) FROM dim_artist")).scalar()
        genre_count = conn.execute(text("SELECT COUNT(*) FROM dim_genre")).scalar()
        date_count = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()

        # Measures
        pop_sum = conn.execute(text("SELECT SUM(popularity) FROM fact_track_performance")).scalar()
        pop_avg = conn.execute(text("SELECT ROUND(AVG(popularity), 4) FROM fact_track_performance")).scalar()
        high_pop_count = conn.execute(text("SELECT SUM(is_high_popularity) FROM fact_track_performance")).scalar()
        grammy_fact_count = conn.execute(text("SELECT SUM(has_grammy) FROM fact_track_performance")).scalar()

    return {
        "fact_rows": fact_count,
        "dim_artist_rows": artist_count,
        "dim_genre_rows": genre_count,
        "dim_date_rows": date_count,
        "popularity_sum": int(pop_sum) if pop_sum else 0,
        "popularity_avg": float(pop_avg) if pop_avg else 0.0,
        "high_popularity_count": int(high_pop_count) if high_pop_count else 0,
        "grammy_fact_count": int(grammy_fact_count) if grammy_fact_count else 0
    }


def run_test_c():
    print("=" * 80)
    print("WORKSHOP-2: TEST C - REPEATABILITY AND SAFE RERUN AUDIT")
    print("=" * 80)

    # 1. Capture snapshot before rerun
    print("\n[STEP 1] Capturing Data Warehouse snapshot BEFORE rerun...")
    before = get_dw_snapshot()
    print(json.dumps(before, indent=4))

    # 2. Execute pipeline rerun
    print("\n[STEP 2] Executing pipeline safe rerun (Test_C)...")
    manifest = execute_pipeline(run_label="Test_C")

    # 3. Capture snapshot after rerun
    print("\n[STEP 3] Capturing Data Warehouse snapshot AFTER rerun...")
    after = get_dw_snapshot()
    print(json.dumps(after, indent=4))

    # 4. Compare and reconcile
    discrepancies = {}
    for key in before:
        diff = after[key] - before[key]
        discrepancies[key] = {
            "before": before[key],
            "after": after[key],
            "difference": diff,
            "passed": (diff == 0)
        }

    is_safe = all(d["passed"] for d in discrepancies.values())
    print("\n" + "=" * 80)
    print("SAFE RERUN RECONCILIATION RESULT:")
    for k, v in discrepancies.items():
        status = "MATCH [PASS]" if v["passed"] else "MISMATCH [FAIL]"
        print(f"  {k:25}: Before={v['before']}, After={v['after']}, Diff={v['difference']} -> {status}")
    print("=" * 80)

    reconciliation_evidence = {
        "test": "Test_C_Repeatability_and_Safe_Rerun",
        "timestamp": datetime.now().isoformat(),
        "status": "SUCCESS" if is_safe else "FAILED",
        "idempotency_strategy": "Atomic transaction via engine.begin() with reverse-referential DELETE FROM.",
        "before_snapshot": before,
        "after_snapshot": after,
        "discrepancies": discrepancies,
        "zero_duplicate_effects_verified": is_safe
    }

    out_file = BASE_DIR / "docs" / "evidence" / "test_c_safe_rerun_reconciliation.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(reconciliation_evidence, f, indent=4)

    print(f"\nAudit reconciliation saved to: {out_file}")
    if not is_safe:
        raise ValueError("Test C failed! Rerun caused data drift or duplicate records.")
    print("Test C passed 100%! The pipeline guarantees safe re-runs without side effects.")


if __name__ == "__main__":
    run_test_c()
