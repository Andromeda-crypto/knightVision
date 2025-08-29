# scripts/enrich_moves_with_practicality.py
import pandas as pd
from pathlib import Path

MOVES_PARQ = "data/processed/moves_sample.parquet"
PRACT_TAB = "data/processed/practicality_table.parquet"
OUT = "data/processed/moves_sample_practicality.parquet"

moves = pd.read_parquet(MOVES_PARQ)
pract = pd.read_parquet(PRACT_TAB)


moves['EloBin'] = moves['mean_elo'].apply(lambda x: f"{int(x//100)*100}-{int(x//100)*100+99}" if pd.notna(x) else "unknown")


pract_lookup = pract.set_index(['ShortSeq','EloBin','Reply'])['freq']

def get_practicality(row):
    key = (row['ShortSeq'], row['EloBin'], row['move_played_uci'])
    try:
        return pract_lookup.loc[key]
    except KeyError:
        return 0.0

moves['practicality_played'] = moves.apply(get_practicality, axis=1)
Path(OUT).parent.mkdir(parents=True, exist_ok=True)
moves.to_parquet(OUT, index=False)
print("Enriched moves saved:", OUT)
