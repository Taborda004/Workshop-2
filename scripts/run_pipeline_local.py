"""
Direct Pipeline Execution & Reliability Testing Harness.

Mirrors the exact Airflow TaskFlow DAG topology:
  [extract_spotify] -> [validate_spotify_raw] \
                                                -> [transform_and_integrate] -> [validate_prepared] -> [load_dw]
  [extract_grammys] -> [validate_grammys_raw] /

Supports:
- Test A: Full Successful Run
- Test B: Controlled Quality Failure Injection
- Test C: Safe Repeatability & Idempotent Rerun
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.extract import extract_spotify_data, extract_grammys_data
from src.transform import transform_and_integrate
from src.validation import run_validation
from src.load import load_data_warehouse
from great_expectations.expectations.metadata_types import FailureSeverity


def execute_pipeline(spotify_input_path: str = None, run_label: str = "Test_A"):
    start_time = datetime.now()
    log_file = BASE_DIR / "docs" / "evidence" / f"{run_label.lower()}_execution.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print(f"EXECUTING WORKSHOP-2 PIPELINE - RUN LABEL: {run_label}")
    print(f"Timestamp: {start_time.isoformat()}")
    print(f"Spotify Source Input: {spotify_input_path or 'data/spotify_dataset.csv'}")
    print("=" * 80)

    run_record = {
        "run_id": f"run_{run_label}_{int(time.time())}",
        "run_label": run_label,
        "start_time": start_time.isoformat(),
        "tasks": {}
    }

    try:
        # Task 1A: extract_spotify
        t0 = time.time()
        print("\n--- TASK: extract_spotify ---")
        raw_sp_path = extract_spotify_data(source_path=spotify_input_path)
        run_record["tasks"]["extract_spotify"] = {
            "state": "SUCCESS",
            "duration_sec": round(time.time() - t0, 2),
            "output": raw_sp_path
        }

        # Task 1B: extract_grammys
        t0 = time.time()
        print("\n--- TASK: extract_grammys ---")
        raw_gr_path = extract_grammys_data()
        run_record["tasks"]["extract_grammys"] = {
            "state": "SUCCESS",
            "duration_sec": round(time.time() - t0, 2),
            "output": raw_gr_path
        }

        # Task 2A: validate_spotify_raw
        t0 = time.time()
        print("\n--- TASK: validate_spotify_raw ---")
        df_sp = pd.read_parquet(raw_sp_path)
        res_sp, max_f_sp, sum_sp = run_validation(
            dataframe=df_sp,
            stage="spotify_raw",
            context_mode="file",
            output_prefix=run_label.lower()
        )

        if max_f_sp == FailureSeverity.CRITICAL:
            run_record["tasks"]["validate_spotify_raw"] = {
                "state": "FAILED",
                "duration_sec": round(time.time() - t0, 2),
                "error": "Critical GX validation failure",
                "max_severity": str(max_f_sp)
            }
            raise ValueError(f"[CIRCUIT BREAKER] Spotify raw validation failed with CRITICAL severity. Blocking downstream tasks.")
        
        run_record["tasks"]["validate_spotify_raw"] = {
            "state": "SUCCESS" if res_sp.success else "WARNING_CONTINUED",
            "duration_sec": round(time.time() - t0, 2),
            "max_severity": str(max_f_sp) if max_f_sp else "NONE"
        }

        # Task 2B: validate_grammys_raw
        t0 = time.time()
        print("\n--- TASK: validate_grammys_raw ---")
        df_gr = pd.read_parquet(raw_gr_path)
        res_gr, max_f_gr, sum_gr = run_validation(
            dataframe=df_gr,
            stage="grammys_raw",
            context_mode="file",
            output_prefix=run_label.lower()
        )

        if max_f_gr == FailureSeverity.CRITICAL:
            run_record["tasks"]["validate_grammys_raw"] = {
                "state": "FAILED",
                "duration_sec": round(time.time() - t0, 2),
                "error": "Critical GX validation failure"
            }
            raise ValueError(f"[CIRCUIT BREAKER] Grammy raw validation failed with CRITICAL severity. Blocking downstream tasks.")

        run_record["tasks"]["validate_grammys_raw"] = {
            "state": "SUCCESS",
            "duration_sec": round(time.time() - t0, 2),
            "max_severity": str(max_f_gr) if max_f_gr else "NONE"
        }

        # Task 3: transform_and_integrate
        t0 = time.time()
        print("\n--- TASK: transform_and_integrate ---")
        prepared_path = transform_and_integrate(raw_sp_path, raw_gr_path)
        run_record["tasks"]["transform_and_integrate"] = {
            "state": "SUCCESS",
            "duration_sec": round(time.time() - t0, 2),
            "output": prepared_path
        }

        # Task 4: validate_prepared
        t0 = time.time()
        print("\n--- TASK: validate_prepared ---")
        df_prep = pd.read_parquet(prepared_path)
        res_prep, max_f_prep, sum_prep = run_validation(
            dataframe=df_prep,
            stage="prepared",
            context_mode="file",
            output_prefix=run_label.lower()
        )

        if max_f_prep == FailureSeverity.CRITICAL:
            run_record["tasks"]["validate_prepared"] = {
                "state": "FAILED",
                "duration_sec": round(time.time() - t0, 2),
                "error": "Critical GX prepared validation failure"
            }
            raise ValueError("[CIRCUIT BREAKER] Prepared validation failed with CRITICAL severity. Blocking DW load.")

        run_record["tasks"]["validate_prepared"] = {
            "state": "SUCCESS",
            "duration_sec": round(time.time() - t0, 2),
            "max_severity": str(max_f_prep) if max_f_prep else "NONE"
        }

        # Task 5: load_dw
        t0 = time.time()
        print("\n--- TASK: load_dw ---")
        load_summary = load_data_warehouse(prepared_path)
        run_record["tasks"]["load_dw"] = {
            "state": "SUCCESS",
            "duration_sec": round(time.time() - t0, 2),
            "summary": load_summary
        }

        run_record["final_state"] = "SUCCESS"
        print(f"\n[PIPELINE COMPLETE] Overall Status: SUCCESS in {round(time.time() - start_time.timestamp(), 2)}s")

    except Exception as exc:
        run_record["final_state"] = "FAILED"
        run_record["error_message"] = str(exc)
        print(f"\n[PIPELINE HALTED] Circuit breaker triggered: {exc}")

        # Mark unreached downstream tasks as BLOCKED/SKIPPED
        all_tasks = ["extract_spotify", "extract_grammys", "validate_spotify_raw", "validate_grammys_raw", "transform_and_integrate", "validate_prepared", "load_dw"]
        for t in all_tasks:
            if t not in run_record["tasks"]:
                run_record["tasks"][t] = {"state": "BLOCKED_UPSTREAM_FAILED"}

    end_time = datetime.now()
    run_record["end_time"] = end_time.isoformat()
    run_record["total_elapsed_sec"] = round((end_time - start_time).total_seconds(), 2)

    evidence_json = BASE_DIR / "docs" / "evidence" / f"{run_label.lower()}_run_manifest.json"
    with open(evidence_json, "w", encoding="utf-8") as f:
        json.dump(run_record, f, indent=4)

    print(f"Run manifest saved to: {evidence_json}")
    return run_record


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "A"
    if mode == "A":
        execute_pipeline(run_label="Test_A")
    elif mode == "B":
        execute_pipeline(spotify_input_path=str(BASE_DIR / "data" / "spotify_dataset_bad.csv"), run_label="Test_B")
    elif mode == "C":
        execute_pipeline(run_label="Test_C")
