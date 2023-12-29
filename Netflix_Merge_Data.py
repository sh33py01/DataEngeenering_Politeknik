import pandas as pd

df_IMDb_movies = pd.read_csv('movies.csv')
df_IMDb_names = pd.read_csv('Netflix_User_Ratings.csv')


df_combined = pd.merge(df_IMDb_movies, df_IMDb_names, on='MovieId')
df_combined.to_csv('Netflix_movies_archive2.csv', index=False)