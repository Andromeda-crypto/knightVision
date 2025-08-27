# scripts/make_move_dataset_sample.py
import chess, chess.pgn, chess.engine
import numpy as np
import pandas as pd
from pathlib import Path

STOCKFISH_PATH = "/Users/omanand/knightvision/engines/stockfish/stockfish-macos-m1-apple-silicon"
PGN_PATH = "/Users/omanand/knightVision/data/raw/lichess_db_standard_rated_2017-02.pgn"
OUT_PATH = "/Users/omanand/knightVision/data/processed/moves_sample.parquet"
MULTIPV = 4
N_GAMES = 50  # small sample for prototype
NODES = 100000

def softmax_from_cp(cps, temp=400.0):
    arr = np.array(cps, dtype=float) / temp
    arr -= arr.max()
    p = np.exp(arr)
    return p / p.sum() if p.sum() != 0 else np.zeros_like(p)

def analyze_position(engine, board):
    info = engine.analyse(board, chess.engine.Limit(nodes=NODES), multipv=MULTIPV)
    if not isinstance(info, list):
        info = [info]
    cand = []
    for entry in info:
        sc = entry["score"].pov(board.turn).score(mate_score=100000)
        mv = entry.get("pv", [None])[0]
        if mv is not None:
            cand.append((mv.uci(), sc))
    cand.sort(key=lambda x: x[1], reverse=True)
    cps = [c for _, c in cand]
    probs = softmax_from_cp(cps)
    entropy = -(probs * np.log(probs + 1e-12)).sum() if len(probs) else None
    return cand, entropy

def iter_games(pgn_path, max_games):
    with open(pgn_path, encoding="utf-8", errors="ignore") as f:
        for i in range(max_games):
            g = chess.pgn.read_game(f)
            if g is None:
                break
            yield g

def main():
    rows = []
    engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
    try:
        for gi, game in enumerate(iter_games(PGN_PATH, N_GAMES)):
            b = game.board()
            H = game.headers
            white_elo = pd.to_numeric(H.get("WhiteElo", None), errors="coerce")
            black_elo = pd.to_numeric(H.get("BlackElo", None), errors="coerce")
            mean_elo = np.nanmean([white_elo, black_elo])

            ply = 0
            # iterate through moves, analyzing before each move
            for mv in game.mainline_moves():
                cand_pre, entropy = analyze_position(engine, b)
                best_uci, best_cp = (cand_pre[0] if cand_pre else (None, None))
                fen_before = b.fen()
                move_played_uci = mv.uci()
                played_cp_pre = None
                if cand_pre:
                    d = {u:c for u,c in cand_pre}
                    played_cp_pre = d.get(move_played_uci, None)

                # push the move to update board (we will later analyze post-play if needed)
                b.push(mv)

                # approximate delta (only if played move present in pre-candidates)
                delta_cp = None
                if best_cp is not None and played_cp_pre is not None:
                    delta_cp = best_cp - played_cp_pre

                rows.append({
                    "game_id": gi,
                    "ply": ply,
                    "side_to_move": "W" if not b.turn else "B",  # note: b.turn is after push; adjust if desired
                    "white_elo": white_elo,
                    "black_elo": black_elo,
                    "mean_elo": mean_elo,
                    "fen_before": fen_before,
                    "move_played_uci": move_played_uci,
                    "sf_best_uci": best_uci,
                    "sf_best_cp": best_cp,
                    "delta_cp": delta_cp,
                    "multipv_k": len(cand_pre),
                    "candidates_uci": [u for u,_ in cand_pre],
                    "candidates_cp": [c for _,c in cand_pre],
                    "choice_entropy": entropy,
                    "move_number": (ply // 2) + 1
                })
                ply += 1
    finally:
        engine.quit()

    df = pd.DataFrame(rows)
    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print("Saved moves sample:", OUT_PATH)

if __name__ == "__main__":
    main()




            