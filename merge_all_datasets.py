import pandas as pd
import dask.dataframe as dd

# File paths
merged_data_path = 'merged_data.csv'
imdb_data_path = 'IMDB_movies_archive1.csv'
netflix_data_path = 'Netflix_movies_archive2.csv'

# Load the datasets in chunks
chunk_size = 10000

# Load merged_data.csv
merged_data_chunks = pd.read_csv(merged_data_path, chunksize=chunk_size)
merged_data = pd.concat(merged_data_chunks)

# Load IMDB_movies_archive1.csv
imdb_chunks = pd.read_csv(imdb_data_path, chunksize=chunk_size)
imdb_data = pd.concat(imdb_chunks)

# Load Netflix_movies_archive2.csv
netflix_chunks = pd.read_csv(netflix_data_path, chunksize=chunk_size)
netflix_data = pd.concat(netflix_chunks)

# Convert columns to lowercase for consistency
merged_data.columns = merged_data.columns.str.lower()
imdb_data.columns = imdb_data.columns.str.lower()
netflix_data.columns = netflix_data.columns.str.lower()

print(netflix_data.columns)

# Update the merge based on the correct column name
merged_data = pd.merge(merged_data, imdb_data, how='left', left_on='movie', right_on='original_title_x')
merged_data = pd.merge(merged_data, netflix_data, how='left', left_on='movie', right_on='movietitle')

# Save the merged dataset to a new CSV file
merged_data.to_csv('merged_dataset.csv', index=False)

# Optional: Parallel processing with Dask
merged_data_dask = dd.from_pandas(merged_data, npartitions=8)  # Adjust the number of partitions as needed
merged_data_dask.compute().to_csv('merged_dataset_dask.csv', index=False)
