import pandas as pd

df_IMDb_movies = pd.read_csv('IMDb movies.csv')
df_IMDb_names = pd.read_csv('IMDb names.csv')


df_combined = pd.merge(df_IMDb_movies, df_IMDb_names, on='imdb_title_id')
df_combined.to_csv('IMDB_movies_archive1.csv', index=False)