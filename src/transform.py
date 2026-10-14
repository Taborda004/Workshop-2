"""
Transformation and Integration Module: Workshop-2 ETL Pipeline.

Single coherent engineering phase that:
1. Cleans missing attribute identities (drops unidentifiable records).
2. Deduplicates Spotify catalog to analytical grain (track_id, track_genre),
   preserving exactly 113,549 valid observations.
3. Normalizes artist names across sources.
4. Aggregates Grammy historical awards into artist-level dimensional metrics.
5. Implements the Integration Contract (N:1 Spotify-to-Grammy matching).
6. Derives analytical attributes (has_grammy, nomination_tier, is_high_popularity, first_grammy_decade).
7. Persists prepared analytical dataset as Parquet for validation and loading.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_WORK_DIR = BASE_DIR / "data" / "work"


def transform_and_integrate(spotify_raw_path: str, grammys_raw_path: str, work_dir: str = None) -> str:
    """
    Execute full data transformation, cleaning, and cross-source integration.

    Returns:
        str: Absolute path to the prepared analytical Parquet file.
    """
    work_path = Path(work_dir) if work_dir else DEFAULT_WORK_DIR
    work_path.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("WORKSHOP-2: TRANSFORMATION AND INTEGRATION STAGE")
    print("=" * 70)

    # 1. Load Raw Sources
    print(f"[TRANSFORM] Loading raw Spotify from: {spotify_raw_path}")
    df_sp = pd.read_parquet(spotify_raw_path)
    raw_sp_count = len(df_sp)

    print(f"[TRANSFORM] Loading raw Grammy from: {grammys_raw_path}")
    df_gr = pd.read_parquet(grammys_raw_path)
    raw_gr_count = len(df_gr)

    # 2. Handle Missing Core Identity
    null_artist_mask = df_sp["artists"].isnull() | df_sp["track_name"].isnull()
    null_rows_count = null_artist_mask.sum()
    df_sp_clean = df_sp[~null_artist_mask].copy()
    print(f"[TRANSFORM] Excluded {null_rows_count} unidentifiable tracks (null artists/track_name).")

    # 3. Deduplicate at Composite Grain (track_id, track_genre)
    dups_before = df_sp_clean.duplicated(subset=["track_id", "track_genre"]).sum()
    df_sp_dedup = df_sp_clean.drop_duplicates(subset=["track_id", "track_genre"]).copy()
    print(f"[TRANSFORM] Removed {dups_before} exact technical duplicate pairs.")
    print(f"[TRANSFORM] Cleaned analytical Spotify catalog: {len(df_sp_dedup):,} rows.")

    # 4. Prepare and Aggregate Grammy Source
    gr_clean = df_gr.copy()
    # Resolve artist from nominee when category is Best New Artist
    bna_mask = gr_clean["artist"].isnull() & (gr_clean["category"] == "Best New Artist")
    gr_clean.loc[bna_mask, "artist"] = gr_clean.loc[bna_mask, "nominee"]

    gr_valid = gr_clean.dropna(subset=["artist"]).copy()
    gr_valid["artist_clean"] = gr_valid["artist"].astype(str).str.strip()
    gr_valid["artist_norm"] = gr_valid["artist_clean"].str.lower()

    # Aggregate to Artist Grain
    grammy_agg = gr_valid.groupby("artist_norm").agg(
        official_artist_name=("artist_clean", "first"),
        total_nominations=("winner", "count"),
        total_wins=("winner", "sum"),
        first_grammy_year=("year", "min")
    ).reset_index()

    grammy_agg["first_grammy_decade"] = (grammy_agg["first_grammy_year"] // 10 * 10).astype(str) + "s"

    def get_tier(n):
        if n == 0:
            return "0"
        elif 1 <= n <= 2:
            return "1-2"
        elif 3 <= n <= 5:
            return "3-5"
        elif 6 <= n <= 10:
            return "6-10"
        else:
            return ">10"

    grammy_agg["nomination_tier"] = grammy_agg["total_nominations"].apply(get_tier)
    print(f"[TRANSFORM] Aggregated {len(grammy_agg):,} distinct recognized Grammy artists.")

    # 5. Cross-Source Integration (Integration Contract: N:1 Matching)
    g_lookup = grammy_agg.set_index("artist_norm").to_dict("index")

    def match_artists(artists_str):
        parts = [p.strip().lower() for p in str(artists_str).split(";")]
        best = None
        for p in parts:
            if p in g_lookup:
                entry = g_lookup[p]
                if best is None or entry["total_nominations"] > best["total_nominations"]:
                    best = entry
        return best

    matches = df_sp_dedup["artists"].apply(match_artists)

    # 6. Derive Analytical Attributes
    df_sp_dedup["has_grammy"] = matches.apply(lambda x: 1 if x is not None else 0).astype(int)
    df_sp_dedup["total_nominations"] = matches.apply(lambda x: x["total_nominations"] if x is not None else 0).astype(int)
    df_sp_dedup["total_wins"] = matches.apply(lambda x: x["total_wins"] if x is not None else 0).astype(int)
    df_sp_dedup["nomination_tier"] = matches.apply(lambda x: x["nomination_tier"] if x is not None else "0")
    df_sp_dedup["first_grammy_year"] = matches.apply(lambda x: int(x["first_grammy_year"]) if x is not None else None)
    df_sp_dedup["first_grammy_decade"] = matches.apply(lambda x: x["first_grammy_decade"] if x is not None else "Sin Grammy")

    # REQ-03 High Popularity Indicator (P75 empirical threshold = 50.0)
    df_sp_dedup["is_high_popularity"] = (df_sp_dedup["popularity"] >= 50.0).astype(int)

    # Primary Artist Name for Dimension (with canonical casing to guarantee case-insensitive uniqueness in MySQL)
    raw_primary = df_sp_dedup["artists"].astype(str).str.split(";").str[0].str.strip()
    canonical_map = raw_primary.groupby(raw_primary.str.lower()).agg(lambda s: s.value_counts().index[0]).to_dict()
    df_sp_dedup["primary_artist"] = raw_primary.str.lower().map(canonical_map)

    # Explicit integer conversions
    df_sp_dedup["duration_ms"] = df_sp_dedup["duration_ms"].astype(int)
    df_sp_dedup["popularity"] = df_sp_dedup["popularity"].astype(int)
    df_sp_dedup["explicit"] = df_sp_dedup["explicit"].astype(int)
    df_sp_dedup["key"] = df_sp_dedup["key"].astype(int)
    df_sp_dedup["mode"] = df_sp_dedup["mode"].astype(int)
    df_sp_dedup["time_signature"] = df_sp_dedup["time_signature"].astype(int)

    prepared_path = work_path / "spotify_grammy_prepared.parquet"
    df_sp_dedup.to_parquet(prepared_path, index=False)

    print("-" * 70)
    print("TRANSFORMATION RECONCILIATION SUMMARY:")
    print(f"  Spotify Raw Ingested:        {raw_sp_count:,}")
    print(f"  Unidentifiable Rows Dropped: {null_rows_count}")
    print(f"  Technical Duplicate Pairs:   {dups_before}")
    print(f"  Prepared Analytical Rows:    {len(df_sp_dedup):,} (Target: 113,549)")
    print(f"  Tracks with Grammy Honors:   {df_sp_dedup['has_grammy'].sum():,}")
    print(f"  Control Group (Sin Grammy):  {(df_sp_dedup['has_grammy'] == 0).sum():,}")
    print(f"  High Popularity Tracks:      {df_sp_dedup['is_high_popularity'].sum():,}")
    print(f"  Output Parquet Persisted:    {prepared_path}")
    print("-" * 70)

    if len(df_sp_dedup) != 113549:
        raise ValueError(f"Deduplication integrity check failed: expected 113,549 rows, got {len(df_sp_dedup)}")

    return str(prepared_path)
