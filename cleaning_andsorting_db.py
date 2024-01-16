import pandas as pd

# Load the merged CSV data
file_path = 'merged_dataset_dask.csv'
df = pd.read_csv(file_path)

# Check for missing values and unify data types
df = df.apply(lambda x: x.str.strip() if x.dtype == "O" else x)
df = df.drop_duplicates()

# Create an append-only table
bronze_table_path = 'bronze_table.csv'
df.to_csv(bronze_table_path, mode='a', header=False, index=False)

# Calculate average for specified columns
average_ratings = df[['rating_x', 'reviews_from_users_x', 'reviews_from_critics_x',
                      'reviews_from_users_y', 'reviews_from_critics_y']].astype(float).mean()

print("Average Ratings:")
print(average_ratings)
