"""
init_gx_assets.py
Initializes the Great Expectations FileDataContext and persists:
- Suites: spotify_raw_suite, grammys_raw_suite, prepared_suite
- Validation Definitions: spotify_raw_validation, grammys_raw_validation, prepared_validation
Matching the project assets structure seen in the workshop guidelines.
"""
from pathlib import Path
import great_expectations as gx

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def build_gx_assets():
    print(f"Initializing GX FileDataContext in: {PROJECT_ROOT / 'gx'}")
    context = gx.get_context(project_root_dir=PROJECT_ROOT)

    # 1. Setup Data Sources & Assets
    # Data Source for Spotify Raw
    try:
        ds_sp = context.data_sources.get("spotify_raw_source")
    except Exception:
        ds_sp = context.data_sources.add_pandas("spotify_raw_source")
    try:
        asset_sp = ds_sp.get_asset("spotify_raw_asset")
    except Exception:
        asset_sp = ds_sp.add_dataframe_asset("spotify_raw_asset")
    try:
        batch_sp = asset_sp.get_batch_definition("spotify_raw_batch")
    except Exception:
        batch_sp = asset_sp.add_batch_definition_whole_dataframe("spotify_raw_batch")

    # Data Source for Grammy Raw
    try:
        ds_gr = context.data_sources.get("grammys_raw_source")
    except Exception:
        ds_gr = context.data_sources.add_pandas("grammys_raw_source")
    try:
        asset_gr = ds_gr.get_asset("grammys_raw_asset")
    except Exception:
        asset_gr = ds_gr.add_dataframe_asset("grammys_raw_asset")
    try:
        batch_gr = asset_gr.get_batch_definition("grammys_raw_batch")
    except Exception:
        batch_gr = asset_gr.add_batch_definition_whole_dataframe("grammys_raw_batch")

    # Data Source for Prepared
    try:
        ds_pr = context.data_sources.get("prepared_source")
    except Exception:
        ds_pr = context.data_sources.add_pandas("prepared_source")
    try:
        asset_pr = ds_pr.get_asset("prepared_asset")
    except Exception:
        asset_pr = ds_pr.add_dataframe_asset("prepared_asset")
    try:
        batch_pr = asset_pr.get_batch_definition("prepared_batch")
    except Exception:
        batch_pr = asset_pr.add_batch_definition_whole_dataframe("prepared_batch")

    # =========================================================================
    # 2. Build and persist Suites
    # =========================================================================

    # Suite A: spotify_raw_suite
    suite_sp = gx.ExpectationSuite(name="spotify_raw_suite")
    suite_sp.add_expectation(gx.expectations.ExpectTableColumnCountToEqual(value=21, severity="critical"))
    suite_sp.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=100000, max_value=150000, severity="critical"))
    suite_sp.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="popularity", min_value=0, max_value=100, severity="critical"))
    suite_sp.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="explicit", value_set=[True, False, 0, 1], severity="critical"))
    suite_sp.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="track_name", mostly=0.99, severity="warning"))
    suite_sp = context.suites.add_or_update(suite_sp)
    print("✓ Persisted suite: gx/expectations/spotify_raw_suite.json")

    # Suite B: grammys_raw_suite
    suite_gr = gx.ExpectationSuite(name="grammys_raw_suite")
    suite_gr.add_expectation(gx.expectations.ExpectTableColumnCountToEqual(value=5, severity="critical"))
    suite_gr.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=4000, max_value=6000, severity="critical"))
    suite_gr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="year", severity="critical"))
    suite_gr.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=1958, max_value=2030, severity="critical"))
    suite_gr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="category", severity="critical"))
    suite_gr.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="winner", value_set=[True, False, 0, 1], severity="critical"))
    suite_gr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="artist", mostly=0.60, severity="warning"))
    suite_gr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="nominee", mostly=0.99, severity="warning"))
    suite_gr = context.suites.add_or_update(suite_gr)
    print("✓ Persisted suite: gx/expectations/grammys_raw_suite.json")

    # Suite C: prepared_suite
    suite_pr = gx.ExpectationSuite(name="prepared_suite")
    suite_pr.add_expectation(gx.expectations.ExpectTableColumnCountToEqual(value=24, severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=100000, max_value=120000, severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="track_id", severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="artist_id", severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="track_genre", severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="popularity", min_value=0, max_value=100, severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="danceability", min_value=0.0, max_value=1.0, severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="energy", min_value=0.0, max_value=1.0, severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["track_id", "track_genre"], severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="has_grammy", value_set=[0, 1], severity="critical"))
    suite_pr.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, severity="critical"))
    suite_pr = context.suites.add_or_update(suite_pr)
    print("✓ Persisted suite: gx/expectations/prepared_suite.json")

    # =========================================================================
    # 3. Build and persist Validation Definitions
    # =========================================================================

    val_sp = gx.ValidationDefinition(name="spotify_raw_validation", data=batch_sp, suite=suite_sp)
    val_sp = context.validation_definitions.add_or_update(val_sp)
    print("✓ Persisted validation: gx/validation_definitions/spotify_raw_validation.json")

    val_gr = gx.ValidationDefinition(name="grammys_raw_validation", data=batch_gr, suite=suite_gr)
    val_gr = context.validation_definitions.add_or_update(val_gr)
    print("✓ Persisted validation: gx/validation_definitions/grammys_raw_validation.json")

    val_pr = gx.ValidationDefinition(name="prepared_validation", data=batch_pr, suite=suite_pr)
    val_pr = context.validation_definitions.add_or_update(val_pr)
    print("✓ Persisted validation: gx/validation_definitions/prepared_validation.json")

    print("\nGX Assets initialization completed successfully.")

if __name__ == "__main__":
    build_gx_assets()
