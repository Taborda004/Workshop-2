"""
transform.py — Transformation and cross-source integration for the
Reliable Music Pipeline.

Input : spotify_raw.parquet  (from extract_spotify)
        grammy_raw.parquet   (from extract_grammys)
Output: music_prepared.parquet

Integration Contract
────────────────────
Grain            : One observation per unique Spotify Track × Genre.
Integration key  : Spotify `primary_artist` ↔ Grammy `artist`
                   (both normalised to lowercase-stripped form)
Cardinality      : Many-to-one (many track-genre observations per artist;
                   one aggregated artist profile)
Unmatched records: Spotify tracks whose primary_artist has no Grammy record
                   → has_grammy=0, grammy_wins=0, grammy_nominations=0,
                     first_grammy_year=NA, first_grammy_decade='Non-Grammy'
Assumptions      : Primary artist is the first token before ";" in the
                   Spotify artists field.
"""

import logging
import os
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Dynamic Path Resolution
PROJECT_ROOT = Path(__file__).resolve().parent.parent

_raw_data_dir = os.environ.get("AIRFLOW_WORK_DIR")
if _raw_data_dir:
    WORK_DIR = Path(_raw_data_dir)
else:
    WORK_DIR = PROJECT_ROOT / "data" / "work"

_raw_output_dir = os.environ.get("AIRFLOW_OUTPUT_DIR")
if _raw_output_dir:
    OUTPUT_DIR = Path(_raw_output_dir)
else:
    OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

PREPARED_PATH = WORK_DIR / "music_prepared.parquet"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_artist(name: pd.Series) -> pd.Series:
    """
    Lowercase + strip normalisation for artist-name matching.
    Collapses multiple whitespaces into a single space.
    """
    return (
        name.astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )


def _extract_primary_artist(artists_col: pd.Series) -> pd.Series:
    """
    Extract the first artist from a semicolon-separated list.
    Example: "Ingrid Michaelson;ZAYN" → "Ingrid Michaelson"
    """
    return (
        artists_col.astype("string")
        .str.split(";")
        .str[0]
        .str.strip()
    )


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — CLEAN SPOTIFY
# ─────────────────────────────────────────────────────────────────────────────

def _clean_spotify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply documented cleaning and standardisation rules to the raw Spotify data.

    Rules applied
    ─────────────
    T-SP-01 Deduplicate (track_id, track_genre) keeping highest-popularity row.
            Justification: Enforces the approved Phase 0 grain: Track × Genre.
            Tracks present in multiple genres retain their multi-genre observations.
    T-SP-02 Extract primary_artist from semicolon-separated artists field.
            Justification: Integration key; Grammy records reference individual artists.
    T-SP-03 Drop rows where primary_artist is null.
            Justification: Cannot integrate without an artist name.
    T-SP-04 Drop rows where track_genre is null.
            Justification: REQ-01, REQ-02, REQ-03 group by genre.
    T-SP-05 Coerce audio feature columns to float; set non-finite values to NaN.
            Justification: Downstream SQL loading requires numeric precision.
    T-SP-06 Normalise explicit to 0/1 integer.
    """
    rows_before = len(df)
    log.info("Cleaning Spotify data. Input rows: %d", rows_before)

    # T-SP-01 Deduplicate on Track x Genre
    df = (
        df.sort_values("popularity", ascending=False)
        .drop_duplicates(subset=["track_id", "track_genre"], keep="first")
        .reset_index(drop=True)
    )
    log.info("T-SP-01 Track x Genre dedup: %d → %d rows", rows_before, len(df))
    print(f"[TRANSFORM] T-SP-01 Track x Genre deduplicated: {rows_before} → {len(df)} rows")

    # T-SP-02 Primary artist
    df["primary_artist"] = _extract_primary_artist(df["artists"])

    # T-SP-03 Drop null primary_artist
    before = len(df)
    df = df.dropna(subset=["primary_artist"]).copy()
    print(f"[TRANSFORM] T-SP-03 dropped null artist: {before - len(df)} rows removed")

    # T-SP-04 Drop null track_genre
    before = len(df)
    df = df.dropna(subset=["track_genre"]).copy()
    print(f"[TRANSFORM] T-SP-04 dropped null genre: {before - len(df)} rows removed")

    # T-SP-05 Audio feature coercion
    audio_cols = [
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    ]
    for col in audio_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # T-SP-06 Explicit as int
    df["explicit"] = df["explicit"].astype(int)

    # Normalised key for matching
    df["_artist_key"] = _normalise_artist(df["primary_artist"])

    log.info("Spotify cleaning complete. Output rows: %d", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 — CLEAN GRAMMY
# ─────────────────────────────────────────────────────────────────────────────

def _clean_grammys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply documented cleaning rules to the raw Grammy data.

    Rules applied
    ─────────────
    T-GR-01 Drop rows where artist is null.
            Justification: 1,840 rows (38%) lack an individual artist credit
            (album/production awards). Documented source limitation.
    T-GR-02 Normalise winner to 0/1 integer.
    T-GR-03 Extract Grammy broad family from category name for genre mapping.
    T-GR-04 Normalise artist name (lowercase + strip) as matching key.
    """
    rows_before = len(df)
    log.info("Cleaning Grammy data. Input rows: %d", rows_before)

    # T-GR-01 Drop null artist
    df = df.dropna(subset=["artist"]).copy()
    log.info("T-GR-01 dropped null artist: %d → %d rows", rows_before, len(df))
    print(f"[TRANSFORM] T-GR-01 Grammy null artist dropped: {rows_before} → {len(df)} rows")

    # T-GR-02 Normalise winner
    df["winner"] = df["winner"].map(
        {True: 1, False: 0, "True": 1, "False": 0, 1: 1, 0: 0}
    ).fillna(0).astype(int)

    # T-GR-03 Grammy category family
    category_map = {
        "pop": "pop", "rock": "rock", "rap": "rap", "hip-hop": "rap",
        "r&b": "r&b", "rhythm": "r&b", "country": "country",
        "jazz": "jazz", "classical": "classical", "latin": "latin",
        "gospel": "gospel", "electronic": "electronic", "dance": "electronic",
        "folk": "folk", "blues": "blues", "reggae": "reggae",
    }

    def _map_family(cat: str) -> str:
        cat_lower = str(cat).lower()
        for keyword, family in category_map.items():
            if keyword in cat_lower:
                return family
        return "other"

    df["grammy_family"] = df["category"].apply(_map_family)

    # T-GR-04 Matching key
    df["_artist_key"] = _normalise_artist(df["artist"])

    log.info("Grammy cleaning complete. Output rows: %d", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — BUILD ARTIST PROFILE & HISTORICAL COHORTS (REQ-03)
# ─────────────────────────────────────────────────────────────────────────────

def _build_grammy_artist_profile(grammy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate Grammy data to one row per normalised artist key.
    Calculates debut year and historical cohort decade for REQ-03.
    """
    profile = (
        grammy_df.groupby("_artist_key", as_index=False)
        .agg(
            grammy_nominations=("_artist_key", "count"),
            grammy_wins=("winner", "sum"),
            first_grammy_year=("year", "min"),
            last_grammy_year=("year", "max"),
            grammy_artist_name=("artist", "first"),
        )
    )
    profile["has_grammy"] = 1

    # REQ-03 Historical cohort decade assignment (e.g., 1960s, 1970s, 2010s)
    profile["first_grammy_decade"] = (
        (profile["first_grammy_year"] // 10 * 10).astype(str) + "s"
    )

    log.info("Grammy artist profiles built: %d unique artists", len(profile))
    print(f"[TRANSFORM] Grammy artist profiles: {len(profile)} unique artists")
    return profile


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — INTEGRATE
# ─────────────────────────────────────────────────────────────────────────────

def _integrate(spotify_df: pd.DataFrame, grammy_profile: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join Spotify tracks with Grammy artist profiles on normalised artist key.

    Cardinality: many (tracks) to one (artist profile).
    Unmatched Spotify tracks receive has_grammy=0 and zero Grammy counters.
    """
    log.info(
        "Integrating. Spotify tracks: %d | Grammy profiles: %d",
        len(spotify_df), len(grammy_profile),
    )

    merged = spotify_df.merge(
        grammy_profile,
        on="_artist_key",
        how="left",
        suffixes=("", "_grammy"),
    )

    # Fill unmatched rows (no Grammy record found for artist)
    merged["has_grammy"]          = merged["has_grammy"].fillna(0).astype(int)
    merged["grammy_nominations"]  = merged["grammy_nominations"].fillna(0).astype(int)
    merged["grammy_wins"]         = merged["grammy_wins"].fillna(0).astype(int)
    merged["first_grammy_year"]   = merged["first_grammy_year"].astype("Int64")
    merged["last_grammy_year"]    = merged["last_grammy_year"].astype("Int64")
    merged["first_grammy_decade"] = merged["first_grammy_decade"].fillna("Non-Grammy").astype("string")

    matched   = (merged["has_grammy"] == 1).sum()
    unmatched = len(merged) - matched

    log.info(
        "Integration complete. Grammy-matched tracks: %d | Unmatched: %d",
        matched, unmatched,
    )
    print(f"[TRANSFORM] Integration: matched={matched} | unmatched={unmatched}")

    # Reconciliation evidence
    print(
        f"[TRANSFORM] Reconciliation: "
        f"spotify_in={len(spotify_df)} | integrated_out={len(merged)} | "
        f"grammy_profiles_used={len(grammy_profile)}"
    )

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 5 — SELECT PREPARED COLUMNS
# ─────────────────────────────────────────────────────────────────────────────

PREPARED_COLUMNS = [
    "track_id", "track_name", "album_name", "primary_artist",
    "track_genre", "popularity", "duration_ms", "explicit",
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature",
    "has_grammy", "grammy_nominations", "grammy_wins",
    "first_grammy_year", "first_grammy_decade", "last_grammy_year",
]


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def transform_and_integrate(spotify_raw_path: str, grammy_raw_path: str) -> str:
    """
    Orchestrate the full transformation and integration pipeline.

    Parameters
    ----------
    spotify_raw_path : str   Path to spotify_raw.parquet
    grammy_raw_path  : str   Path to grammy_raw.parquet

    Returns
    -------
    str : Absolute path to music_prepared.parquet
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Transform & integrate started.")
    print("[TRANSFORM] Starting transformation and integration.")

    # Load raw files
    spotify_df = pd.read_parquet(spotify_raw_path)
    grammy_df  = pd.read_parquet(grammy_raw_path)

    log.info("Loaded Spotify raw: %d rows | Grammy raw: %d rows",
             len(spotify_df), len(grammy_df))

    # Phase 1 — clean Spotify
    spotify_clean = _clean_spotify(spotify_df)

    # Phase 2 — clean Grammy
    grammy_clean = _clean_grammys(grammy_df)

    # Phase 3 — build Grammy artist profile with historical cohorts
    grammy_profile = _build_grammy_artist_profile(grammy_clean)

    # Phase 4 — integrate
    integrated = _integrate(spotify_clean, grammy_profile)

    # Phase 5 — select output columns
    prepared = integrated[PREPARED_COLUMNS].copy()
    prepared.to_parquet(PREPARED_PATH, index=False)

    log.info(
        "Transform & integrate complete. "
        "Prepared rows: %d | Output: %s",
        len(prepared), PREPARED_PATH,
    )
    print(f"[TRANSFORM] Done. Prepared rows={len(prepared)} | path={PREPARED_PATH}")

    return str(PREPARED_PATH)
