import json
import csv
from pathlib import Path

# Step 1: Merge JSON files into one big JSON
merged_json = []

for i in range(1, 8):
    file_name = f'part-{i:02d}.json'
    with open(file_name, 'r') as json_file:
        data = json.load(json_file)
        merged_json.extend(data)

# Step 2: Convert merged JSON to CSV
csv_file_name = 'merged_data.csv'

with open(csv_file_name, 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)

    # Write header
    header = ["review_id", "reviewer", "movie", "rating", "review_summary", "review_date", "spoiler_tag", "review_detail"]
    csv_writer.writerow(header)

    # Write data
    for review in merged_json:
        row = [review.get(key, '') for key in header]
        csv_writer.writerow(row)
