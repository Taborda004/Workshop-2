"""
generate_profiling_charts.py
Generates visual evidence charts for the data profiling report and Phase 0/Phase 1 validation.
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

os.makedirs("docs/evidence", exist_ok=True)
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'

# Load datasets
spotify_path = "data/spotify_dataset.csv" if os.path.exists("data/spotify_dataset.csv") else "spotify_dataset.csv"
grammy_path = "data/the_grammy_awards.csv" if os.path.exists("data/the_grammy_awards.csv") else "the_grammy_awards.csv"

df_spotify = pd.read_csv(spotify_path)
df_grammy = pd.read_csv(grammy_path)

# Chart 1: Popularity Distribution & P75 Threshold
fig, ax = plt.subplots(figsize=(10, 5))
sns.histplot(df_spotify['popularity'], bins=50, kde=True, color='#1DB954', ax=ax)
p75 = df_spotify['popularity'].quantile(0.75)
p50 = df_spotify['popularity'].median()
ax.axvline(p75, color='#E91429', linestyle='--', linewidth=2, label=f'P75 Threshold ({p75:.1f}) - High Popularity')
ax.axvline(p50, color='#191414', linestyle=':', linewidth=1.5, label=f'Median ({p50:.1f})')
ax.set_title("Spotify Popularity Distribution & Thresholds (N=114,000)", fontsize=13, fontweight='bold')
ax.set_xlabel("Popularity (0 - 100)", fontsize=11)
ax.set_ylabel("Track Count", fontsize=11)
ax.legend(fontsize=10)
plt.tight_layout()
fig.savefig("docs/evidence/chart1_popularity_distribution.png", dpi=200)
plt.close(fig)
print("Saved chart1_popularity_distribution.png")

# Chart 2: Grammy Recognitions Across Decades (1958 - 2019)
df_grammy['year'] = pd.to_numeric(df_grammy['year'], errors='coerce')
df_grammy = df_grammy.dropna(subset=['year'])
df_grammy['decade'] = (df_grammy['year'] // 10 * 10).astype(int).astype(str) + "s"
decade_counts = df_grammy.groupby(['decade', 'winner']).size().unstack(fill_value=0)

fig, ax = plt.subplots(figsize=(10, 5))
decade_counts.plot(kind='bar', stacked=True, color=['#7B8A8B', '#D4AF37'], ax=ax)
ax.set_title("Grammy Nominations and Wins by Decade (1958-2019, N=4,810)", fontsize=13, fontweight='bold')
ax.set_xlabel("Decade", fontsize=11)
ax.set_ylabel("Total Nominations / Awards", fontsize=11)
ax.legend(["Nominee Only", "Grammy Winner"], fontsize=10)
plt.xticks(rotation=0)
plt.tight_layout()
fig.savefig("docs/evidence/chart2_grammy_decades.png", dpi=200)
plt.close(fig)
print("Saved chart2_grammy_decades.png")

# Chart 3: Audio Features Comparison (Grammy Recognized vs Comparison Group)
# Match artists
grammy_artists = set(df_grammy['artist'].dropna().str.lower().str.strip().unique())
df_spotify['is_grammy'] = df_spotify['artists'].dropna().apply(
    lambda a: any(part.strip().lower() in grammy_artists for part in str(a).split(';'))
)

audio_cols = ['danceability', 'energy', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence']
means_comparison = df_spotify.groupby('is_grammy')[audio_cols].mean().T
means_comparison.columns = ['Comparison Group (No Grammy)', 'Grammy Recognized']

fig, ax = plt.subplots(figsize=(10, 5))
means_comparison.plot(kind='bar', color=['#2C3E50', '#F39C12'], ax=ax)
ax.set_title("Audio Features: Grammy Recognized vs. Comparison Group (REQ-02)", fontsize=13, fontweight='bold')
ax.set_xlabel("Audio Dimension", fontsize=11)
ax.set_ylabel("Mean Normalized Value (0 - 1.0)", fontsize=11)
ax.legend(fontsize=10)
plt.xticks(rotation=30, ha='right')
plt.tight_layout()
fig.savefig("docs/evidence/chart3_audio_features_comparison.png", dpi=200)
plt.close(fig)
print("Saved chart3_audio_features_comparison.png")

# Chart 4: Top Genres by Grammy Popularity Lift
genre_stats = df_spotify.groupby(['track_genre', 'is_grammy'])['popularity'].agg(['mean', 'count']).unstack()
genre_stats = genre_stats[genre_stats[('count', True)] >= 30] # At least 30 Grammy tracks in genre
genre_lift = (genre_stats[('mean', True)] - genre_stats[('mean', False)]).sort_values(ascending=False).head(15)

fig, ax = plt.subplots(figsize=(11, 6))
bars = ax.barh(genre_lift.index[::-1], genre_lift.values[::-1], color='#3498DB')
ax.axvline(0, color='black', linewidth=0.8)
ax.set_title("Top 15 Genres by Popularity Lift (Grammy Mean - Comparison Mean) [REQ-01]", fontsize=13, fontweight='bold')
ax.set_xlabel("Popularity Score Difference", fontsize=11)
ax.set_ylabel("Musical Genre", fontsize=11)
for bar in bars:
    width = bar.get_width()
    ax.text(width + 0.3 if width >= 0 else width - 1.5, bar.get_y() + bar.get_height()/2,
            f"{width:+.1f}", va='center', fontsize=9, fontweight='bold')
plt.tight_layout()
fig.savefig("docs/evidence/chart4_genre_popularity_lift.png", dpi=200)
plt.close(fig)
print("Saved chart4_genre_popularity_lift.png")
