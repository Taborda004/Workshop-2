"""
reliable_music_pipeline.py — Main Airflow DAG for Workshop 2.

Course  : ETL - Data Engineering and Artificial Intelligence
Airflow : 3.1.8  (TaskFlow API — from airflow.sdk import dag, task)
GX      : 1.3.x

Analytical Requirements
───────────────────────
R1 – Association between Grammy recognition and Spotify popularity,
     varying by music genre and number of recognitions.
R2 – Predominant audio features of Grammy artists vs non-Grammy,
     broken down by genre.
R3 – Popularity comparison: Grammy artists vs non-Grammy artists.

Architecture
────────────
SPOTIFY CSV          GRAMMY MYSQL SOURCE
      ↓                      ↓
 extract_spotify        extract_grammys
      ↓                      ↓
validate_spotify_raw   validate_grammys_raw
           ↓          ↓
     (both gates must pass)
     transform_and_integrate
              ↓
        validate_prepared
              ↓
           load_dw
              ↓
        DATA WAREHOUSE (MySQL music_dw)
              ↓
       KPIs / Power BI Dashboard

Reliability Controls
────────────────────
• Two independent raw validation gates (Critical blocks downstream).
• One prepared validation gate (Critical blocks load).
• Selective retries: transient network/DB tasks retry 3×; pure
  computation tasks do not retry (deterministic output).
• Safe rerun: TRUNCATE + INSERT strategy — rerun cannot silently
  accumulate duplicates in the DW.
• Failure evidence: GX results serialised to JSON in output/gx_results/.
• Load evidence: load_summary.json in output/.

Inter-task communication: FILE PATHS only (Parquet files under
/opt/airflow/data/work). DataFrames are never passed through the
Airflow metadata store.
"""

import sys
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from great_expectations.expectations.metadata_types import FailureSeverity

# Make src/ importable from within the Airflow container
sys.path.insert(0, "/opt/airflow/src")

log = logging.getLogger(__name__)

# ============================================================
# DEFAULT TASK ARGUMENTS
# ============================================================

default_args = {
    "owner": "etl-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Selective retries: DB/network tasks get 3 retries; pure
    # computation tasks override this with retries=0.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="reliable_music_pipeline",
    description=(
        "Workshop 2 — Reliable batch pipeline integrating Spotify and Grammy data "
        "to answer R1 (Grammy vs popularity), R2 (audio features), R3 (popularity comparison)."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["workshop-2", "etl", "gx", "spotify", "grammy", "mysql"],
    doc_md=__doc__,
)
def reliable_music_pipeline():

    # ========================================================
    # BRANCH A — EXTRACT SPOTIFY
    # ========================================================
    @task(
        task_id="extract_spotify",
        retries=3,          # Retries: file I/O may fail transiently in Docker
        retry_delay=timedelta(minutes=1),
    )
    def extract_spotify_task() -> str:
        """
        Read the Spotify CSV from the mounted Docker volume and save
        a raw Parquet working file.

        Returns the path to spotify_raw.parquet.
        """
        from extract import extract_spotify
        return extract_spotify()

    # ========================================================
    # BRANCH A — RAW VALIDATION: SPOTIFY
    # ========================================================
    @task(
        task_id="validate_spotify_raw",
        retries=0,          # Deterministic: same data → same result
    )
    def validate_spotify_raw_task(spotify_raw_path: str) -> None:
        """
        Apply raw Spotify data quality rules (DQ-SP-01 to DQ-SP-08).

        CRITICAL failure → raises ValueError → blocks transform_and_integrate.
        WARNING failure  → logged; pipeline continues per documented policy.
        """
        import pandas as pd
        from validation import run_gx_validation

        df = pd.read_parquet(spotify_raw_path)
        result, max_failure = run_gx_validation(df, stage="spotify_raw")

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Spotify raw validation CRITICAL failure. "
                "Transformation is blocked. "
                "Check output/gx_results/spotify_raw_validation_result.json for details."
            )

        if max_failure == FailureSeverity.WARNING:
            log.warning(
                "Spotify raw validation WARNING. "
                "Documented policy: pipeline continues. "
                "Review gx_results/spotify_raw_validation_result.json."
            )
            print("[VALIDATE] Spotify raw WARNING — continuing per policy.")

        print("[VALIDATE] Spotify raw gate PASSED.")
        log.info("Spotify raw validation gate passed. Success=%s", result.success)

    # ========================================================
    # BRANCH B — EXTRACT GRAMMY
    # ========================================================
    @task(
        task_id="extract_grammys",
        retries=3,          # Retries: DB connection may fail transiently
        retry_delay=timedelta(minutes=2),
    )
    def extract_grammys_task() -> str:
        """
        Query the Grammy Awards MySQL source database and save a raw
        Parquet working file.

        Returns the path to grammy_raw.parquet.
        """
        from extract import extract_grammys
        return extract_grammys()

    # ========================================================
    # BRANCH B — RAW VALIDATION: GRAMMY
    # ========================================================
    @task(
        task_id="validate_grammys_raw",
        retries=0,
    )
    def validate_grammys_raw_task(grammy_raw_path: str) -> None:
        """
        Apply raw Grammy data quality rules (DQ-GR-01 to DQ-GR-06).

        CRITICAL failure → raises ValueError → blocks transform_and_integrate.
        WARNING failure  → logged; pipeline continues.
        """
        import pandas as pd
        from validation import run_gx_validation

        df = pd.read_parquet(grammy_raw_path)
        result, max_failure = run_gx_validation(df, stage="grammy_raw")

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Grammy raw validation CRITICAL failure. "
                "Transformation is blocked. "
                "Check output/gx_results/grammy_raw_validation_result.json for details."
            )

        if max_failure == FailureSeverity.WARNING:
            log.warning(
                "Grammy raw validation WARNING — continuing per policy. "
                "Note: ~38%% of Grammy rows have null artist (known, documented limitation)."
            )
            print("[VALIDATE] Grammy raw WARNING — continuing per policy.")

        print("[VALIDATE] Grammy raw gate PASSED.")
        log.info("Grammy raw validation gate passed. Success=%s", result.success)

    # ========================================================
    # CONVERGENCE — TRANSFORM & INTEGRATE
    # ========================================================
    @task(
        task_id="transform_and_integrate",
        retries=0,          # Deterministic transformation; no transient failures expected
    )
    def transform_and_integrate_task(
        spotify_raw_path: str,
        grammy_raw_path: str,
    ) -> str:
        """
        Clean both sources, build Grammy artist profiles, and left-join
        Spotify tracks with Grammy recognition metrics.

        Returns the path to music_prepared.parquet.
        """
        from transform import transform_and_integrate
        return transform_and_integrate(spotify_raw_path, grammy_raw_path)

    # ========================================================
    # PREPARED VALIDATION GATE
    # ========================================================
    @task(
        task_id="validate_prepared",
        retries=0,
    )
    def validate_prepared_task(prepared_path: str) -> None:
        """
        Apply prepared-data quality rules (DQ-PREP-01 to DQ-PREP-10).

        Gate question: Did transformation produce data suitable for loading?

        CRITICAL failure → raises ValueError → blocks load_dw.
        """
        import pandas as pd
        from validation import run_gx_validation

        df = pd.read_parquet(prepared_path)
        result, max_failure = run_gx_validation(df, stage="prepared")

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Prepared-data validation CRITICAL failure. "
                "DW load is blocked. "
                "Check output/gx_results/prepared_validation_result.json for details."
            )

        print("[VALIDATE] Prepared data gate PASSED.")
        log.info("Prepared validation gate passed. Success=%s", result.success)

    # ========================================================
    # LOAD TO DATA WAREHOUSE
    # ========================================================
    @task(
        task_id="load_dw",
        retries=3,          # Retries: DB write may fail transiently
        retry_delay=timedelta(minutes=2),
    )
    def load_dw_task(prepared_path: str) -> dict:
        """
        Load validated prepared data into the MySQL music_dw star schema.

        Safe-rerun strategy: TRUNCATE + INSERT.
        Returns a load summary dict with row counts.
        """
        from load import load_to_dw
        return load_to_dw(prepared_path)

    # ========================================================
    # WORKFLOW WIRING
    # ========================================================

    # Branch A
    spotify_raw_path = extract_spotify_task()
    spotify_raw_val  = validate_spotify_raw_task(spotify_raw_path)

    # Branch B
    grammy_raw_path  = extract_grammys_task()
    grammy_raw_val   = validate_grammys_raw_task(grammy_raw_path)

    # Convergence: transform only starts after BOTH raw gates pass
    prepared_path = transform_and_integrate_task(spotify_raw_path, grammy_raw_path)
    [spotify_raw_val, grammy_raw_val] >> prepared_path

    # Prepared gate: load only starts after prepared validation passes
    prepared_val  = validate_prepared_task(prepared_path)
    load_result   = load_dw_task(prepared_path)
    prepared_val  >> load_result


# Make the DAG discoverable by Airflow
reliable_music_pipeline()
