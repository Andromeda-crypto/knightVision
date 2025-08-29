# scripts/build_practicality_table.py
import pandas as pd
import numpy as np
from pathlib import Path

GAMES_CSV = "data/processed/games_5000.csv"
OUT_PATH = "data/processed/practicality_table.parquet"

def short_sequence(moves_str, ply=6):
    if not isinstance(moves_str, str): return ""
    parts = moves_str.split()
    return " ".join(parts[:ply])

def mean_elo(row):
    try:
        we = float(row['WhiteElo'])
        be = float(row['BlackElo'])
        return np.nanmean([we, be])
    except:
        return np.nan

def elo_bin(mean_elo, width=100):
    if pd.isna(mean_elo):
        return "unknown"
    b = int(mean_elo // width) * width
    return f"{b}-{b+width-1}"

df = pd.read_csv(GAMES_CSV)
df['MeanElo'] = df.apply(mean_elo, axis=1)
df['ShortSeq'] = df['Moves'].apply(lambda x: short_sequence(x, ply=6))
df['EloBin'] = df['MeanElo'].apply(lambda x: elo_bin(x, width=100))

# Extract the reply (next move after short sequence)
def get_reply(moves_str, reply_index=6):
    parts = str(moves_str).split()
    return parts[reply_index] if len(parts) > reply_index else None

df['Reply'] = df['Moves'].apply(lambda m: get_reply(m, reply_index=6))

# Filter and count
pract = df.dropna(subset=['Reply']).groupby(['ShortSeq','EloBin','Reply']).size().reset_index(name='count')
# Convert to relative freq
pract['total'] = pract.groupby(['ShortSeq','EloBin'])['count'].transform('sum')
pract['freq'] = pract['count'] / pract['total']

Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
pract.to_parquet(OUT_PATH, index=False)
print("Practicality table saved:", OUT_PATH)
