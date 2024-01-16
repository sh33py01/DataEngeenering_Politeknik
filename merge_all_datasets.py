import pandas as pd

# Load datasets
imdb_df = pd.read_csv('IMDB_movies_archive1.csv', 
                                            usecols=['imdb_title_id','title_x', 'year_x', 'genre_x', 'avg_vote_x', 'country_x','reviews_from_users_x','reviews_from_critics_x','votes_x','director_x',])
merged_data_df = pd.read_csv('merged_data.csv', usecols=['movie', 'rating'])
netflix_df = pd.read_csv('Netflix_movies_archive2.csv', usecols=['MovieTitle', 'Rating'])

# Rename 'MovieTitle' to 'title' in the Netflix dataset
netflix_df.rename(columns={'MovieTitle': 'title'}, inplace=True)

# Normalize ratings
imdb_df['avg_vote_x'] = imdb_df['avg_vote_x'] / 2
merged_data_df['rating'] = merged_data_df['rating'] / 2

# Group by 'title' and calculate the average rating for Netflix dataset
netflix_avg_rating = netflix_df.groupby('title')['Rating'].mean().reset_index()
netflix_avg_rating.rename(columns={'title': 'title', 'Rating': 'avg_rating_netflix'}, inplace=True)

# Rename columns for consistency and rename avg_vote_x to avg_rating_imdb
imdb_df.rename(columns={'title_x': 'title', 'avg_vote_x': 'avg_rating_imdb'}, inplace=True)
merged_data_df.rename(columns={'movie': 'title'}, inplace=True)

# Merge datasets using 'title' as the key
combined_df = pd.merge(netflix_avg_rating, imdb_df, on='title', how='outer')
combined_df = pd.merge(combined_df, merged_data_df, on='title', how='outer')

# Handling missing values for categorical columns
categorical_columns = combined_df.select_dtypes(include=['category']).columns
for col in categorical_columns:
    combined_df[col] = combined_df[col].cat.add_categories('Unknown').fillna('Unknown')

# Handling missing values for numeric columns
numeric_columns = ['avg_rating_netflix', 'avg_rating_imdb', 'rating']
for col in numeric_columns:
    if col in combined_df.columns:
        combined_df[col].fillna(combined_df[col].mean(), inplace=True)

# Calculating the overall average rating
combined_df['average_rating'] = combined_df[numeric_columns].mean(axis=1)

# Removing duplicates based on 'title' and 'imdb_title_id'
combined_df.drop_duplicates(subset=['title', 'imdb_title_id'], inplace=True)

# Saving the final dataset
cleaned_merged_dataset_path = 'cleaned_merged_dataset_optimized_finale.csv'
combined_df.to_csv(cleaned_merged_dataset_path, index=False)

print("Cleaned and merged dataset saved to:", cleaned_merged_dataset_path)
