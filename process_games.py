import pandas as pd
from knightvision.parser import parse_pgn
from pathlib import Path

Path("data/processed").mkdir(exist_ok=True)

pgn_path = "data/raw/lichess_db_standard_rated_2017-02.pgn"
df = parse_pgn(pgn_path, max_games= 5000)

processed_path = "data/processed/games_5000.csv"

df.to_csv(processed_path, index=False)

print(f"Saved games to {processed_path}")
