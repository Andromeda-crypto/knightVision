

import pandas as pd
import chess.pgn
import chess.engine
import io

df = pd.read_csv('/Users/omanand/knightVision/data/processed/games_5000.csv')

total_games = len(df)
print("Total games:", total_games)

result_counts = df['Result'].value_counts()
print("\nGame results distribution:")
print(result_counts)

df['MoveCount'] = df['Moves'].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
average_length = df['MoveCount'].mean()
print(f"\nAverage game length: {average_length:.2f} moves")

df['WhiteElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce')
df['BlackElo'] = pd.to_numeric(df['BlackElo'], errors='coerce')

print('Average White Elo: ', df['WhiteElo'].mean())
print('Average Black Elo: ', df['BlackElo'].mean())

top_openings = df['ECO'].value_counts().head(5)
print("\nTop 5 openings (by ECO code):", top_openings.to_dict())

popular_opening_games = df[df['ECO'] == 'C20']
def opening_sequence(moves, n=7):
    move_list = moves.split()
    return " ".join(move_list[:n])

popular_opening_games['OpeningSequence'] = popular_opening_games['Moves'].apply(lambda x: opening_sequence(x, n=7))
top_variations = popular_opening_games['OpeningSequence'].value_counts().head(5)

print('Top Variations: ')
print(top_variations)


# Error profiling with Stockfish


# Path to Stockfish
STOCKFISH_PATH = "/Users/omanand/knightvision/engines/stockfish/stockfish-macos-m1-apple-silicon"

def classify_error(prev_eval, new_eval, turn):
   
    if prev_eval is None or new_eval is None:
        return None
    diff = (new_eval - prev_eval) if turn == chess.WHITE else (prev_eval - new_eval)
    if diff > 300:
        return "Blunder"
    elif diff > 150:
        return "Mistake"
    elif diff > 50:
        return "Inaccuracy"
    else:
        return "Good"

# Pick a sample of games for analysis
sampled_df = df.sample(5, random_state=42)  

error_records = []

with chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH) as engine:
    for idx, row in sampled_df.iterrows():
        moves_str = row['Moves']
        if not isinstance(moves_str, str):
            continue

        game_pgn = " ".join(moves_str.split())
        pgn = io.StringIO(f"[Event '?']\n\n{game_pgn}")
        game = chess.pgn.read_game(pgn)

        if game is None:
            continue

        board = game.board()
        prev_eval = None
        errors = {"Inaccuracy": 0, "Mistake": 0, "Blunder": 0}

        for move in game.mainline_moves():
           
            info = engine.analyse(board, chess.engine.Limit(time=0.05))
            prev_eval = info["score"].white().score(mate_score=10000)

            board.push(move)

          
            info_after = engine.analyse(board, chess.engine.Limit(time=0.05))
            new_eval = info_after["score"].white().score(mate_score=10000)

            category = classify_error(prev_eval, new_eval, board.turn)
            if category in errors:
                errors[category] += 1

        avg_rating = (row['WhiteElo'] + row['BlackElo']) / 2
        error_records.append({
            "GameID": idx,
            "AvgRating": avg_rating,
            **errors
        })

error_df = pd.DataFrame(error_records)

if not error_df.empty:
    bins = [800, 1200, 1600, 2000, 2400]
    labels = ["<1200", "1200-1600", "1600-2000", "2000-2400"]
    error_df['RatingBand'] = pd.cut(error_df['AvgRating'], bins=bins, labels=labels)

    error_stats = error_df.groupby('RatingBand')[['Inaccuracy', 'Mistake', 'Blunder']].mean()
    print("\nError profiling by rating band (avg per game):")
    print(error_stats)
else:
    print("\nNo error profiling results (empty dataset).")


  