"""
create_bad_data.py — Generate a controlled failure dataset for Test B.

This script MUST be run OUTSIDE the DAG file to adhere to Airflow best practices.
It creates spotify_dataset_bad.csv by injecting a deliberate CRITICAL
violation: one popularity value set to 150 (outside [0, 100]).

This deterministic violation triggers a CRITICAL failure in:
    Rule DQ-SP-02: ExpectColumnValuesToBeBetween(column="popularity", min=0, max=100)
which immediately raises ValueError in validate_spotify_raw and BLOCKS
downstream execution (transform_and_integrate, validate_prepared, load_dw).

Usage (from the project root):
    python scripts/create_bad_data.py
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Source path resolution
if (PROJECT_ROOT / "data" / "spotify_dataset.csv").exists():
    GOOD_CSV = PROJECT_ROOT / "data" / "spotify_dataset.csv"
else:
    GOOD_CSV = PROJECT_ROOT / "spotify_dataset.csv"

# Target bad paths (both data/ and project root for Docker & local compatibility)
BAD_CSV_DATA = PROJECT_ROOT / "data" / "spotify_dataset_bad.csv"
BAD_CSV_ROOT = PROJECT_ROOT / "spotify_dataset_bad.csv"


def create_bad_data() -> None:
    print(f"[TEST B PREP] Reading source CSV: {GOOD_CSV}")

    if not GOOD_CSV.exists():
        print(f"ERROR: {GOOD_CSV} not found.")
        return

    df = pd.read_csv(GOOD_CSV, low_memory=False)
    print(f"[TEST B PREP] Original rows read: {len(df)}")

    # Inject CRITICAL violation: popularity = 150 (valid range is 0–100)
    original_val = df.loc[df.index[0], "popularity"]
    df.loc[df.index[0], "popularity"] = 150

    print(
        f"[TEST B PREP] Injected Critical violation at row index 0: "
        f"column='popularity' | original={original_val} → injected=150 "
        f"(violates Rule DQ-SP-02: range [0, 100])"
    )

    # Write bad CSVs
    df.to_csv(BAD_CSV_DATA, index=False)
    df.to_csv(BAD_CSV_ROOT, index=False)

    print(f"[TEST B PREP] Bad dataset written to:")
    print(f"  → {BAD_CSV_DATA}")
    print(f"  → {BAD_CSV_ROOT}")
    print()
    print("Execution steps for Test B:")
    print("  1. In .env or docker-compose.yaml, set:")
    print("     SPOTIFY_CSV_PATH=/opt/airflow/data/spotify_dataset_bad.csv")
    print("  2. Trigger reliable_music_pipeline in Airflow.")
    print("  3. Observe validate_spotify_raw FAIL with CRITICAL severity.")
    print("  4. Verify that transform_and_integrate and downstream tasks are BLOCKED/SKIPPED.")
    print("  5. Inspect output/gx_results/spotify_raw_validation_result.json for failure evidence.")


if __name__ == "__main__":
    create_bad_data()
