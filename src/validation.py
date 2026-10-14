"""
validation.py — Great Expectations validation helper for the Reliable Music Pipeline.

Provides run_gx_validation(dataframe, stage) with three stages:
    "spotify_raw"   — raw Spotify data quality gates
    "grammy_raw"    — raw Grammy data quality gates
    "prepared"      — post-transformation integrated dataset gate

Each stage enforces rules derived from profiling evidence and analytical
requirements (REQ-01, REQ-02, REQ-03). Severity levels follow the workshop policy:
    CRITICAL      → blocks the affected downstream path
    WARNING       → logs and continues under documented policy
    INFORMATIONAL → monitoring context / trend signal; never blocks

GX results are serialised to JSON under OUTPUT_DIR/gx_results/ and mirrored
to docs/evidence/ so that verifiable audit evidence is preserved.
"""

import json
import logging
import os
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.expectations.metadata_types import FailureSeverity

log = logging.getLogger(__name__)

# Dynamic Path Resolution
PROJECT_ROOT = Path(__file__).resolve().parent.parent

_raw_output_dir = os.environ.get("AIRFLOW_OUTPUT_DIR")
if _raw_output_dir:
    OUTPUT_DIR = Path(_raw_output_dir)
else:
    OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

GX_RESULTS_DIR = OUTPUT_DIR / "gx_results"
EVIDENCE_DIR   = PROJECT_ROOT / "docs" / "evidence"


def _persist_result(result, stage: str) -> None:
    """Serialize the GX ValidationResult to JSON for evidence preservation."""
    GX_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    result_path = GX_RESULTS_DIR / f"{stage}_validation_result.json"
    evidence_path = EVIDENCE_DIR / f"{stage}_validation_result.json"

    try:
        result_dict = result.to_json_dict()
        with open(result_path, "w", encoding="utf-8") as fh:
            json.dump(result_dict, fh, indent=2, default=str)
        # Mirror copy to docs/evidence for audit traceability
        with open(evidence_path, "w", encoding="utf-8") as fh:
            json.dump(result_dict, fh, indent=2, default=str)

        log.info("GX result persisted to %s and mirrored to %s", result_path, evidence_path)
        print(f"[GX] Result saved → {result_path}")
    except Exception as exc:  # pylint: disable=broad-except
        log.warning("Could not persist GX result: %s", exc)


def get_gx_context():
    """
    Get a GX DataContext: prefers FileDataContext (persisted under PROJECT_ROOT/gx)
    to maintain project assets as recommended, or falls back to ephemeral context.
    """
    gx_dir = PROJECT_ROOT / "gx"
    if gx_dir.exists() and (gx_dir / "great_expectations.yml").exists():
        try:
            return gx.get_context(project_root_dir=PROJECT_ROOT)
        except Exception as exc:
            log.warning("Could not open FileDataContext at %s: %s. Using ephemeral.", gx_dir, exc)
    return gx.get_context(mode="ephemeral")


def run_gx_validation(dataframe: pd.DataFrame, stage: str):
    """
    Build or load a GX context, add/update the stage-specific Expectation Suite,
    run validation, persist the result, and return (result, max_failure).

    Parameters
    ----------
    dataframe : pd.DataFrame
        The dataset to validate.
    stage : str
        One of "spotify_raw", "grammy_raw", "grammys_raw", "prepared".

    Returns
    -------
    result : ValidationResult
    max_failure : FailureSeverity | None
        None if all expectations passed; otherwise the highest severity failed.
    """
    valid_stages = {"spotify_raw", "grammy_raw", "grammys_raw", "prepared"}
    if stage not in valid_stages:
        raise ValueError(f"Unknown validation stage '{stage}'. Must be one of {valid_stages}.")

    log.info("Starting GX validation — stage: %s | rows: %d", stage, len(dataframe))

    context = get_gx_context()
    normalized_stage = "grammys_raw" if stage in ("grammy_raw", "grammys_raw") else stage

    # Ensure Data Source, Asset, and Batch Definition
    try:
        data_source = context.data_sources.get(f"{normalized_stage}_source")
    except Exception:
        data_source = context.data_sources.add_pandas(name=f"{normalized_stage}_source")

    try:
        data_asset = data_source.get_asset(f"{normalized_stage}_asset")
    except Exception:
        data_asset = data_source.add_dataframe_asset(name=f"{normalized_stage}_asset")

    try:
        batch_def = data_asset.get_batch_definition(f"{normalized_stage}_batch")
    except Exception:
        batch_def = data_asset.add_batch_definition_whole_dataframe(f"{normalized_stage}_batch")

    suite = gx.ExpectationSuite(name=f"{normalized_stage}_suite")

    # ── SPOTIFY RAW GATES ────────────────────────────────────────────────────
    if stage == "spotify_raw":

        # DQ-SP-01 | Completeness | track_id not null — Critical
        # Business key; cannot uniquely track stream observations without it.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_id",
                severity="critical",
            )
        )

        # DQ-SP-02 | Validity | popularity range [0, 100] — Critical
        # Metric used in REQ-01, REQ-03; out-of-range values corrupt KPIs.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
                severity="critical",
            )
        )

        # DQ-SP-03 | Completeness | artists not null — Critical
        # Cross-source integration matching key.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artists",
                mostly=0.99,
                severity="critical",
            )
        )

        # DQ-SP-04 | Completeness | track_genre not null — Critical
        # Grain and grouping dimension for REQ-01, REQ-02.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_genre",
                severity="critical",
            )
        )

        # DQ-SP-05 | Validity | danceability range [0.0, 1.0] — Critical
        # Audio feature used in REQ-02 sonic profile.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="danceability",
                min_value=0.0,
                max_value=1.0,
                severity="critical",
            )
        )

        # DQ-SP-06 | Validity | energy range [0.0, 1.0] — Critical
        # Audio feature used in REQ-02 sonic profile.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="energy",
                min_value=0.0,
                max_value=1.0,
                severity="critical",
            )
        )

        # DQ-SP-07 | Uniqueness | track_id uniqueness — Warning
        # Multi-genre tracks share track_id; deduplication to Track x Genre handled in transform.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="track_id",
                severity="warning",
            )
        )

        # DQ-SP-08 | Completeness | album_name not null — Warning
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="album_name",
                mostly=0.99,
                severity="warning",
            )
        )

        # DQ-SP-09 | Timeliness / Operational | duration_ms > 10,000 — Informational
        # Monitoring signal for potential corrupt or truncated audio track files.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="duration_ms",
                min_value=10000,
                severity="warning",
            )
        )

    # ── GRAMMY RAW GATES ─────────────────────────────────────────────────────
    elif stage == "grammy_raw":

        # DQ-GR-01 | Completeness | year not null — Critical
        # Dimension key for temporal timeline and historical cohorts.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="year",
                severity="critical",
            )
        )

        # DQ-GR-02 | Validity | year range [1958, 2030] — Critical
        # Historical Grammy ceremony range; future/invalid years indicate corruption.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1958,
                max_value=2030,
                severity="critical",
            )
        )

        # DQ-GR-03 | Completeness | category not null — Critical
        # Award category defining genre family mapping.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="category",
                severity="critical",
            )
        )

        # DQ-GR-04 | Validity | winner in allowed set — Critical
        # Winner flag drives grammy_wins count.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="winner",
                value_set=[True, False, 0, 1],
                severity="critical",
            )
        )

        # DQ-GR-05 | Completeness | artist not null (mostly 0.60) — Warning
        # Profiling shows ~38% rows lack artist credit (album/producer awards). Filtered in transform.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artist",
                mostly=0.60,
                severity="warning",
            )
        )

        # DQ-GR-06 | Completeness | nominee not null — Warning
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="nominee",
                mostly=0.99,
                severity="warning",
            )
        )

    # ── PREPARED DATA GATE (Post-Transformation) ─────────────────────────────
    elif stage == "prepared":

        # DQ-PREP-01 | Completeness | track_id not null — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_id",
                severity="critical",
            )
        )

        # DQ-PREP-02 | Completeness | primary_artist not null — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="primary_artist",
                severity="critical",
            )
        )

        # DQ-PREP-03 | Completeness | track_genre not null — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_genre",
                severity="critical",
            )
        )

        # DQ-PREP-04 | Validity | popularity range [0, 100] — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
                severity="critical",
            )
        )

        # DQ-PREP-05 | Validity | has_grammy in {0, 1} — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="has_grammy",
                value_set=[0, 1],
                severity="critical",
            )
        )

        # DQ-PREP-06 | Validity | grammy_nominations >= 0 — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="grammy_nominations",
                min_value=0,
                severity="critical",
            )
        )

        # DQ-PREP-07 | Validity | danceability range [0.0, 1.0] — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="danceability",
                min_value=0.0,
                max_value=1.0,
                severity="critical",
            )
        )

        # DQ-PREP-08 | Validity | energy range [0.0, 1.0] — Critical
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="energy",
                min_value=0.0,
                max_value=1.0,
                severity="critical",
            )
        )

        # DQ-PREP-09 | Uniqueness | Compound key (track_id, track_genre) — Critical
        # Strictly enforces the declared Phase 0 grain: Track x Genre.
        suite.add_expectation(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["track_id", "track_genre"],
                severity="critical",
            )
        )

        # DQ-PREP-10 | Completeness | Table row count > 0 — Critical
        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=1,
                severity="critical",
            )
        )

    # Register or update suite in context
    try:
        suite = context.suites.add_or_update(suite)
    except Exception:
        suite = context.suites.add(suite)

    # ── Run validation ────────────────────────────────────────────────────────
    validation_def = gx.ValidationDefinition(
        name=f"{normalized_stage}_validation",
        data=batch_def,
        suite=suite,
    )
    try:
        validation_def = context.validation_definitions.add_or_update(validation_def)
    except Exception:
        validation_def = context.validation_definitions.add(validation_def)

    result = validation_def.run(
        batch_parameters={"dataframe": dataframe},
        result_format={"result_format": "SUMMARY"},
    )

    max_failure = result.get_max_severity_failure()

    # Print summary to task logs
    print(f"[GX] stage={stage} | success={result.success} | max_failure={max_failure}")
    print(f"[GX] statistics={result.statistics}")

    # Persist result as verifiable evidence
    _persist_result(result, stage)

    log.info(
        "GX validation complete — stage=%s | success=%s | max_failure=%s",
        stage, result.success, max_failure,
    )

    return result, max_failure
