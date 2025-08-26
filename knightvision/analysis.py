import pandas as pd

df = pd.read_csv('/Users/omanand/knightVision/data/processed/games_5000.csv')

total_games = len(df)
print("total games:", total_games)

result_counts = df['Result'].value_counts()
print("\nGame results distribution:")
print(result_counts)

df['MoveCount'] = df['Moves'].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)

average_length = df['MoveCount'].mean()
print(f"\nAverage game length: {average_length:.2f} moves")

df['WhiteElo'] = pd.to_numeric(df['WhiteElo'], errors='coerce')
df['BlackElo'] = pd.to_numeric(df['BlackElo'], errors='coerce')

print('Average white elo: ',{df['WhiteElo'].mean()})
print('Average Black elo: ',{df['BlackElo'].mean()})

top_openings = df['ECO'].value_counts().head(5)
print("\nTop 5 openings (by ECO code):", top_openings.to_dict())



popular_opening_games = df[df['ECO'] == 'C20']
def opening_sequence(moves, n= 7):
    move_list = moves.split()
    return " ".join(move_list[:n])

popular_opening_games['OpeningSequence'] = popular_opening_games['Moves'].apply(lambda x : opening_sequence(x,n=7))
top_variations = popular_opening_games['OpeningSequence'].value_counts().head(5)

print('Top Variations: ')
print(top_variations)

  