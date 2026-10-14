"""
Creates data/spotify_dataset_bad.csv by injecting a deterministic Critical violation:
popularity = 150.0 (violating DQ-RAW-SP-01: popularity must be between 0 and 100).
"""

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

src_path = DATA_DIR / "spotify_dataset.csv"
bad_path = DATA_DIR / "spotify_dataset_bad.csv"

print(f"Reading source dataset: {src_path}")
df = pd.read_csv(src_path)

# Inject critical violation on the very first track
print(f"Original popularity of track 0 ({df.loc[0, 'track_id']}): {df.loc[0, 'popularity']}")
df.loc[0, "popularity"] = 150.0
print(f"Injected invalid popularity: {df.loc[0, 'popularity']} (violates DQ-RAW-SP-01: [0, 100])")

df.to_csv(bad_path, index=False)
print(f"Controlled failure dataset created at: {bad_path}")
