# parse.py 
'''
takes a PGN file,

loads all games inside it,

and returns them in a clean data structure we can reuse later.
'''

import chess.pgn
from pathlib import Path
import pandas as pd

def parse_pgn(pgn_path: str, max_games: int = 5):
    games = []
    pgn = open(pgn_path)
    for i in range(max_games):
        game = chess.pgn.read_game(pgn)
        if game is None:
            break

        headers = game.headers
        moves = " ".join([move.uci() for move in game.mainline_moves()])
        game_dict = {
            "Event": headers.get("Event","Unknown"),
            "Site": headers.get("Site","Unknown"),
            "Date": headers.get("Date", "Unknown"),
            "White": headers.get("White", "Unknown"),
            "Black": headers.get("Black","Unknown"),
            "Result": headers.get("Result","Unknown"),
            "ECO": headers.get("ECO", "Unknown"),
            "WhiteElo" : headers.get("WhiteElo", "Unknown"),
            "BlackElo" : headers.get("BlackElo", "Unknown"),
            "Moves": moves
                    }
        
        games.append(game_dict)

    return pd.DataFrame(games)





if __name__ == "__main__":
    pgn_path = "/Users/omanand/knightVision/data/raw/lichess_db_standard_rated_2017-02.pgn"

    parse_pgn(pgn_path, max_games=5000)

