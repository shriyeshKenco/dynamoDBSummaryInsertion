# Initialize DynamoDB table
table = dynamodb.Table('summary_operations_per_hour')


# Function to insert data into DynamoDB
def insert_into_dynamodb(data):
    with table.batch_writer() as batch:
        for index, row in data.iterrows():
            batch.put_item(
                Item={
                    'TableName': row['TableName'],
                    'TimeStamp': int(row['TimeStamp']),
                    'Creations': int(row['Creations']),
                    'Updates': int(row['Updates']),
                    'Deletions': int(row['Deletions']),
                    'HourType': row['HourType']
                }
            )


# Iterate through the days and hours
current_datetime = start_datetime
for day in range(num_days):
    day_start_time = time.time()

    for hour in range(total_hours):
        num_creations, num_updates, num_deletes = 0, 0, 0
        hour_type = "Active" if active_hours_start <= current_datetime.hour < inactive_hours_start else "Inactive"

        if active_hours_start <= current_datetime.hour < inactive_hours_start:
            # Active hours
            num_creations = max(0, int(np.random.normal(350, 65)))
            num_updates = max(0, int(np.random.normal(80, 20)))
            num_deletes = max(0, int(np.random.normal(30, 15)))
        else:
            # Inactive hours
            num_creations = max(0, int(np.random.normal(70, 20)))
            num_updates = max(0, int(np.random.normal(30, 5)))
            num_deletes = max(0, int(np.random.normal(10, 5)))

        # Create records
        new_records_df = create_records(num_creations, current_datetime)
        df = pd.concat([df, new_records_df], ignore_index=True)

        # Update records
        update_records(df, num_updates, current_datetime)

        # Delete records
        delete_records(df, num_deletes, current_datetime)

        # Store summarized data
        timestamp_key = current_datetime.strftime('%Y%m%d%H%M')
        summary_entry = {
            'TableName': 'Synthetic_Logistics_Data',
            'TimeStamp': int(timestamp_key),
            'Creations': num_creations,
            'Updates': num_updates,
            'Deletions': num_deletes,
            'HourType': hour_type
        }
        summary_data = pd.concat([summary_data, pd.DataFrame([summary_entry])], ignore_index=True)

        # Insert the summary entry into DynamoDB
        table.put_item(Item=summary_entry)

        # Move to the next hour
        current_datetime += timedelta(hours=1)

    day_end_time = time.time()
    print(f"Day {day + 1} processing time: {day_end_time - day_start_time:.2f} seconds")

# End timing the entire process
end_time = time.time()
print(f"Total processing time: {end_time - start_time:.2f} seconds")

# Save summarized data to CSV
summary_data.to_csv("summary_operations_per_hour.csv", index=False)

# Save main DataFrame to CSV for later use
df.to_csv("synthetic_logistics_data_with_operations.csv", index=False)

# Display the first few rows of the DataFrame
print(df.head())
