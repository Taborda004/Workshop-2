"""
Validation Module: Great Expectations Gates for Workshop-2 ETL Pipeline.

Implements automated quality validation gates for:
1. spotify_raw (CSV source ingestion)
2. grammys_raw (MySQL relational source ingestion)
3. prepared (integrated analytical dataset before Data Warehouse loading)

Supports both FileDataContext (persisted under gx/) and Ephemeral mode.
Saves audit JSON evidence to docs/evidence/.
"""

import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import great_expectations as gx
from great_expectations.expectations.metadata_types import FailureSeverity

BASE_DIR = Path(__file__).resolve().parent.parent
GX_DIR = BASE_DIR / "gx"
EVIDENCE_DIR = BASE_DIR / "docs" / "evidence"


def get_gx_context(mode: str = "file"):
    """
    Retrieve or initialize the Great Expectations Context.
    Defaults to FileDataContext persisted under gx/.
    Falls back to ephemeral if running in constrained environments.
    """
    if mode == "file" and GX_DIR.exists():
        try:
            return gx.get_context(mode="file", project_root_dir=str(BASE_DIR))
        except Exception as e:
            print(f"[WARN] Failed to load FileDataContext ({e}). Falling back to ephemeral context.")
            return gx.get_context(mode="ephemeral")
    return gx.get_context(mode="ephemeral")


def build_and_register_suite(context, stage: str):
    """
    Build and register the ExpectationSuite for the given pipeline stage.
    """
    suite_name = f"{stage}_expectation_suite"
    
    # If suite already exists in context with expectations, reuse it safely without disk churn
    try:
        existing_suite = context.suites.get(suite_name)
        if existing_suite and len(existing_suite.expectations) > 0:
            return existing_suite
    except Exception:
        pass

    expectations = []
    if stage == "spotify_raw":
        # DQ-RAW-SP-01: Popularity bounds [0, 100] (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
                mostly=1.0,
                severity="critical"
            )
        )
        # DQ-RAW-SP-02: Track ID completeness and validity (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_id",
                severity="critical"
            )
        )
        # DQ-RAW-SP-03: Artists completeness (Warning: 1 null tolerated at raw stage)
        expectations.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artists",
                mostly=0.999,
                severity="warning"
            )
        )
        # DQ-RAW-SP-04: Normalized audio features bounds [0.0, 1.0] (Critical)
        for col in ["danceability", "energy", "valence"]:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col,
                    min_value=0.0,
                    max_value=1.0,
                    mostly=1.0,
                    severity="critical"
                )
            )
        # DQ-RAW-SP-05: Positive duration (Warning)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="duration_ms",
                min_value=1000,
                mostly=1.0,
                severity="warning"
            )
        )

    elif stage == "grammys_raw":
        # DQ-RAW-GR-01: Award Year validity (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1958,
                max_value=2030,
                mostly=1.0,
                severity="critical"
            )
        )
        # DQ-RAW-GR-02: Winner flag validity (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="winner",
                value_set=[True, False, 1, 0],
                mostly=1.0,
                severity="critical"
            )
        )
        # DQ-RAW-GR-03: Category completeness (Warning)
        expectations.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="category",
                mostly=0.99,
                severity="warning"
            )
        )

    elif stage == "prepared":
        # DQ-PREP-01: Composite uniqueness of (track_id, track_genre) (Critical)
        expectations.append(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["track_id", "track_genre"],
                severity="critical"
            )
        )
        # DQ-PREP-02: Artists and Track Name completeness post-transformation (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artists",
                severity="critical"
            )
        )
        expectations.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_name",
                severity="critical"
            )
        )
        # DQ-PREP-03: Nomination Tier valid domain (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="nomination_tier",
                value_set=["0", "1-2", "3-5", "6-10", ">10"],
                severity="critical"
            )
        )
        # DQ-PREP-04: Binary flags validity (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="has_grammy",
                value_set=[0, 1],
                severity="critical"
            )
        )
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="is_high_popularity",
                value_set=[0, 1],
                severity="critical"
            )
        )
        # DQ-PREP-05: Decade cohort validity (Critical)
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="first_grammy_decade",
                value_set=["1950s", "1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "Sin Grammy"],
                severity="critical"
            )
        )

    else:
        raise ValueError(f"Unknown validation stage: {stage}")

    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    suite = gx.ExpectationSuite(name=suite_name, expectations=expectations)
    return context.suites.add(suite)


def run_validation(dataframe: pd.DataFrame, stage: str, context_mode: str = "file", output_prefix: str = ""):
    """
    Execute a Great Expectations validation gate for a specific stage.

    Returns:
        tuple: (result, max_failure_severity, summary_dict)
    """
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    context = get_gx_context(mode=context_mode)

    ds_name = f"{stage}_datasource"
    asset_name = f"{stage}_asset"
    batch_name = f"{stage}_batch_def"
    val_def_name = f"{stage}_validation_def"

    # Add or retrieve pandas data source
    try:
        data_source = context.data_sources.add_pandas(name=ds_name)
    except Exception:
        data_source = context.data_sources.get(ds_name)

    # Add or retrieve dataframe asset
    try:
        data_asset = data_source.add_dataframe_asset(name=asset_name)
    except Exception:
        data_asset = data_source.get_asset(asset_name)

    # Add batch definition
    try:
        batch_def = data_asset.add_batch_definition_whole_dataframe(batch_name)
    except Exception:
        batch_def = data_asset.get_batch_definition(batch_name)

    # Build suite
    suite = build_and_register_suite(context, stage)

    # Add validation definition
    try:
        context.validation_definitions.delete(val_def_name)
    except Exception:
        pass

    val_def = gx.ValidationDefinition(
        name=val_def_name,
        data=batch_def,
        suite=suite
    )
    val_def = context.validation_definitions.add(val_def)

    # Execute validation
    result = val_def.run(
        batch_parameters={"dataframe": dataframe},
        result_format={"result_format": "SUMMARY"}
    )

    max_failure = result.get_max_severity_failure()
    stats = result.statistics

    # Build structured summary for logging and machine-readable evidence
    summary = {
        "stage": stage,
        "timestamp": datetime.now().isoformat(),
        "total_rows_evaluated": len(dataframe),
        "overall_success": result.success,
        "max_failure_severity": str(max_failure) if max_failure else "NONE",
        "evaluated_expectations": stats.get("evaluated_expectations", 0),
        "successful_expectations": stats.get("successful_expectations", 0),
        "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
        "success_percent": stats.get("success_percent", 0.0),
        "expectation_results": []
    }

    # Extract individual per-rule results
    desc = result.describe_dict()
    for exp_res in desc.get("expectations", []):
        exp_type = exp_res.get("expectation_type", "")
        success = exp_res.get("success", False)
        kwargs = exp_res.get("kwargs", {})
        summary["expectation_results"].append({
            "expectation": exp_type,
            "column": kwargs.get("column", str(kwargs.get("column_list", "N/A"))),
            "success": success,
            "details": exp_res.get("result", {})
        })

    # Save evidence file
    file_tag = f"{output_prefix}_{stage}" if output_prefix else stage
    evidence_path = EVIDENCE_DIR / f"{file_tag}_validation_result.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4)

    print(f"\n[GX GATE: {stage.upper()}]")
    print(f"  Rows Evaluated:        {len(dataframe):,}")
    print(f"  Validation Success:    {result.success}")
    print(f"  Max Failure Severity:  {max_failure}")
    print(f"  Success Rate:          {stats.get('success_percent', 0.0):.2f}%")
    print(f"  Evidence Preserved:    {evidence_path.name}")

    return result, max_failure, summary
