# parse.py 
'''
takes a PGN file,

loads all games inside it,

and returns them in a clean data structure we can reuse later.
'''

import chess.pgn
from pathlib import Path

def parse_pgn(pgn_path: str, max_games: int = 5):
    pgn_file = Path(pgn_path)
    if not pgn_file.exists():
        raise FileNotFoundError(f"PGN file not found at {pgn_path}")
    
    with open(pgn_file, encoding="utf-8") as pgn:
        for i in range(max_games):
            game = chess.pgn.read_game(pgn) 
            if game is None:
                print("No more games found")
                break
            # extract metadata

            headers = game.headers
            print(f"\nGame {i+1}:")
            print(f"Event   : {headers.get('Event', 'Unknown')}")
            print(f"Site    : {headers.get('Site', 'Unknown')}")
            print(f"Date    : {headers.get('Date', 'Unknown')}")
            print(f"White   : {headers.get('White', 'Unknown')} ({headers.get('WhiteElo', '?')})")
            print(f"Black   : {headers.get('Black', 'Unknown')} ({headers.get('BlackElo', '?')})")
            print(f"Result  : {headers.get('Result', 'Unknown')}")
            print(f"Opening : {headers.get('Opening', 'Unknown')}")


if __name__ == "__main__":
    pgn_path = "knightVision/data/raw/lichess_db_standard_rated_2017-02.pgn"
    parse_pgn(pgn_path, max_games=5)
