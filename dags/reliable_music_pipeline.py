"""
Reliable Music Pipeline - Workshop-2: Building a Reliable Batch Data Pipeline

Course: ETL (G01) - Data Engineering and Artificial Intelligence
Institution: Universidad Autónoma de Occidente
Airflow: 3.1.8 (TaskFlow API via `from airflow.sdk import dag, task`)
Great Expectations: 1.24.x
Database: MySQL 8.0 (Source: grammy_source, Data Warehouse: spotify_grammy_dw)

Architectural Pattern:
Two independently observable source branches (Spotify CSV and Grammy MySQL DB)
converge through strict Great Expectations raw validation gates into a single
transformation/integration stage, followed by a prepared validation gate,
and an atomic transactional load into the dimensional Data Warehouse.

Task interfaces exchange lightweight Parquet file paths, avoiding bulk data transfer
through Airflow metadata storage (XCom).
"""

import os
import shutil
from datetime import timedelta
from pathlib import Path
import pandas as pd
import pendulum

from airflow.sdk import dag, task
from great_expectations.expectations.metadata_types import FailureSeverity

# Pipeline core modules
from src.extract import extract_spotify_data, extract_grammys_data
from src.transform import transform_and_integrate as run_transform_and_integrate
from src.validation import run_validation
from src.load import load_data_warehouse

# ============================================================================
# GLOBAL DIRECTORY & SOURCE CONFIGURATION
# ============================================================================

BASE_DIR = Path("/opt/airflow") if Path("/opt/airflow/data").exists() else Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
WORK_DIR = DATA_DIR / "work"

# Input dataset selection (permits controlled failure testing for Test B)
SPOTIFY_SOURCE_FILENAME = os.getenv("SPOTIFY_SOURCE_FILENAME", "spotify_dataset.csv")
SPOTIFY_SOURCE_PATH = DATA_DIR / SPOTIFY_SOURCE_FILENAME


# ============================================================================
# DAG DEFINITION
# ============================================================================

@dag(
    dag_id="reliable_music_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "gx", "workshop-2", "reliable-pipeline", "spotify", "grammy"],
    doc_md=__doc__,
)
def reliable_music_pipeline():

    # ========================================================================
    # STAGE 1A: EXTRACT SPOTIFY (CSV SOURCE)
    # Failure Policy: Bounded retries for transient filesystem I/O
    # ========================================================================
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def extract_spotify() -> str:
        """
        Extract catalog data from Spotify CSV.
        Returns serialized raw Parquet file path.
        """
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        print(f"[TASK: extract_spotify] Ingesting from: {SPOTIFY_SOURCE_PATH}")
        raw_path = extract_spotify_data(source_path=str(SPOTIFY_SOURCE_PATH), work_dir=str(WORK_DIR))
        return str(raw_path)

    # ========================================================================
    # STAGE 1B: EXTRACT GRAMMY AWARDS (MySQL SOURCE DATABASE)
    # Failure Policy: Bounded retries for transient relational database connection
    # ========================================================================
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def extract_grammys() -> str:
        """
        Extract Grammy Awards from operational MySQL source table grammy_source.grammy_awards_raw.
        Returns serialized raw Parquet file path.
        """
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        print("[TASK: extract_grammys] Ingesting from MySQL grammy_source.grammy_awards_raw...")
        raw_path = extract_grammys_data(work_dir=str(WORK_DIR))
        return str(raw_path)

    # ========================================================================
    # STAGE 2A: VALIDATE SPOTIFY RAW (GREAT EXPECTATIONS GATE)
    # Failure Policy: retries=0 (Deterministic data quality gate)
    # ========================================================================
    @task(retries=0)
    def validate_spotify_raw(raw_path: str):
        """
        Apply spotify_raw_suite expectations before transformation.
        Critical violations halt downstream processing.
        Warning-level violations log diagnostic information and continue under policy.
        """
        print(f"[TASK: validate_spotify_raw] Validating: {raw_path}")
        df = pd.read_parquet(raw_path)
        result, max_failure, summary = run_validation(
            dataframe=df,
            stage="spotify_raw",
            context_mode="file",
            output_prefix="dag_run"
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                f"[CIRCUIT BREAKER] Spotify raw validation failed with CRITICAL severity. "
                f"Transformation is blocked. Details: {summary.get('unsuccessful_expectations')} violations detected."
            )

        if max_failure == FailureSeverity.WARNING:
            print("[POLICY INFO] Spotify raw validation contains WARNING-level notifications. Policy permits continuation.")

        print("[OK] Spotify raw validation gate passed.")
        return raw_path

    # ========================================================================
    # STAGE 2B: VALIDATE GRAMMYS RAW (GREAT EXPECTATIONS GATE)
    # Failure Policy: retries=0 (Deterministic data quality gate)
    # ========================================================================
    @task(retries=0)
    def validate_grammys_raw(raw_path: str):
        """
        Apply grammys_raw_suite expectations before transformation.
        Critical violations halt downstream processing.
        """
        print(f"[TASK: validate_grammys_raw] Validating: {raw_path}")
        df = pd.read_parquet(raw_path)
        result, max_failure, summary = run_validation(
            dataframe=df,
            stage="grammys_raw",
            context_mode="file",
            output_prefix="dag_run"
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                f"[CIRCUIT BREAKER] Grammy raw validation failed with CRITICAL severity. "
                f"Transformation is blocked."
            )

        print("[OK] Grammy raw validation gate passed.")
        return raw_path

    # ========================================================================
    # STAGE 3: TRANSFORM & INTEGRATE (CONVERGENCE STAGE)
    # Failure Policy: retries=0 (Deterministic business logic transformation)
    # ========================================================================
    @task(retries=0)
    def transform_and_integrate(spotify_raw_path: str, grammys_raw_path: str) -> str:
        """
        Harmonize, clean, deduplicate to (track_id, track_genre), and integrate both sources.
        Preserves exactly 113,549 analytical observations.
        Returns path to prepared Parquet file.
        """
        print("[TASK: transform_and_integrate] Running cross-source integration contract...")
        prepared_path = run_transform_and_integrate(
            spotify_raw_path=spotify_raw_path,
            grammys_raw_path=grammys_raw_path,
            work_dir=str(WORK_DIR)
        )
        return str(prepared_path)

    # ========================================================================
    # STAGE 4: VALIDATE PREPARED (GREAT EXPECTATIONS GATE)
    # Failure Policy: retries=0 (Deterministic data quality gate)
    # ========================================================================
    @task(retries=0)
    def validate_prepared(prepared_path: str):
        """
        Verify post-transformation invariants, composite uniqueness of (track_id, track_genre),
        and analytical feature domains before loading into Data Warehouse.
        """
        print(f"[TASK: validate_prepared] Validating analytical readiness: {prepared_path}")
        df = pd.read_parquet(prepared_path)
        result, max_failure, summary = run_validation(
            dataframe=df,
            stage="prepared",
            context_mode="file",
            output_prefix="dag_run"
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                f"[CIRCUIT BREAKER] Prepared analytical validation failed with CRITICAL severity. "
                f"Data Warehouse load is blocked."
            )

        print("[OK] Prepared validation gate passed. Analytical dataset certified for loading.")
        return prepared_path

    # ========================================================================
    # STAGE 5: LOAD DATA WAREHOUSE (TRANSACTIONAL ACID LOAD)
    # Failure Policy: Bounded retries for transient database write lock / connectivity
    # ========================================================================
    @task(retries=3, retry_delay=timedelta(seconds=15))
    def load_dw(prepared_path: str) -> dict:
        """
        Load certified prepared data into MySQL star schema (spotify_grammy_dw)
        using atomic transactions and reverse referential teardown for safe reruns.
        """
        print(f"[TASK: load_dw] Loading Data Warehouse from: {prepared_path}")
        summary = load_data_warehouse(prepared_parquet_path=prepared_path)
        print(f"[OK] Successfully loaded {summary['tables']['fact_track_performance']:,} facts to MySQL Data Warehouse.")
        return summary

    # ========================================================================
    # PIPELINE TOPOLOGY & EXPLICIT DEPENDENCY GRAPH
    # ========================================================================

    # Source Extractions
    raw_spotify_path = extract_spotify()
    raw_grammys_path = extract_grammys()

    # Independent Raw Validation Gates
    val_sp_gate = validate_spotify_raw(raw_spotify_path)
    val_gr_gate = validate_grammys_raw(raw_grammys_path)

    # Transformation & Integration: Converges both validated branches
    prepared_path = transform_and_integrate(raw_spotify_path, raw_grammys_path)
    val_sp_gate >> prepared_path
    val_gr_gate >> prepared_path

    # Prepared Validation Gate: Blocks loading if transformation invariants fail
    val_prep_gate = validate_prepared(prepared_path)

    # Dimensional Data Warehouse Load
    load_summary = load_dw(prepared_path)
    val_prep_gate >> load_summary


# Make DAG discoverable by Apache Airflow
reliable_music_pipeline()
