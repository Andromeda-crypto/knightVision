import pandas as pd
import numpy as np
from pathlib import Path

GAMES_CSV = "data/processed/games_5000.csv"
OUT_PATH = "data/processed/practicality_table.parquet"


def short_sequence(moves_str, ply = 6):
    if not isinstance(moves_str, str): return ""
    return " ".join(moves_str.spplit()[:ply])

def elo_bin(mean_elo):
    if pd.isna(mean_elo) : return "Unknown"
    b = int(mean_elo // 100) * 100

    return f"{b}-{b+99}"


df = pd.read_csv(GAMES_CSV)
df['MeanElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce').fillna(0)
df['MeanElo'] = df[['WhiteElo','BlackElo']].apply(lambda r: np.nanmean([pd.to_numeric(r['WhiteElo'], errors='coerce'), pd.to_numeric(r['BlackElo'], errors='coerce')]), axis=1)
df['ShortSeq'] = df['Moves'].apply(lambda x: short_sequence(x, ply=6))
df['EloBin'] = df['MeanElo'].apply(lambda x: elo_bin(x))



def get_reply(moves_str, reply_index=6):
    parts = str(moves_str).split()
    if len(parts) > reply_index:
        return parts[reply_index]
    return None

df['Reply'] = df['Moves'].apply(lambda m: get_reply(m, reply_index=6))
pract = df.dropna(subset=['Reply']).groupby(['ShortSeq','EloBin','Reply']).size().reset_index(name='count')


total = pract.groupby(['ShortSeq','EloBin'])['count'].transform('sum')
pract['freq'] = pract['count'] / total


Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
pract.to_parquet(OUT_PATH, index=False)
print("Saved practicality table to", OUT_PATH)