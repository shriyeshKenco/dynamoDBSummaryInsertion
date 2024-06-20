import boto3
from boto3.dynamodb.conditions import Key
from davinci.services.auth import get_secret
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

# Initialize a session using Amazon DynamoDB
boto3_login = {
    "verify": False,
    "service_name": 'dynamodb',
    "region_name": 'us-east-1',
    "aws_access_key_id": get_secret("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": get_secret("AWS_SECRET_ACCESS_KEY")
}
dynamodb = boto3.resource(**boto3_login)

# Define the table name
table_name = 'summary_operations_per_hour'

# Check if the table already exists
existing_tables = [table.name for table in dynamodb.tables.all()]

if table_name not in existing_tables:
    # Create the DynamoDB table
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'TableName',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'TimeStamp',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'TableName',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'TimeStamp',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

else:
    # If the table already exists, get the table resource
    table = dynamodb.Table(table_name)

print("Table status:", table.table_status)

# Load the initial dataset
df = pd.read_csv("synthetic_logistics_data.csv", parse_dates=['Created', 'Modified'])

# Ensure Day and Hour columns are added to the dataframe
df['Day'] = pd.NaT
df['Hour'] = pd.NaT

# Parameters
start_datetime = datetime(2024, 6, 1, 6, 0)  # Starting at 06:00 on June 1st, 2024
num_days = 28  # Generate data for 4 weeks
active_hours_start = 6  # Active hours start at 0600
inactive_hours_start = 22  # Inactive hours start at 2200
total_hours = 24  # Total hours in a day

# Estimate total number of records to be created based on means
total_estimated_creations = num_days * (
        np.random.normal(350, 65) * (inactive_hours_start - active_hours_start) +
        np.random.normal(70, 20) * (total_hours - (inactive_hours_start - active_hours_start)))
total_estimated_creations = int(total_estimated_creations * 1.5)  # Adding buffer

# Start IDs from 2001
new_ids = np.arange(2001, 2001 + total_estimated_creations)

# Index to keep track of the current ID
current_id_index = 0

# Configurations
CONFIG = {
    'DetailNumber': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records), 'null_rate': 0.1},
    'LoadNumber': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records), 'null_rate': 0.1},
    'LotNumber': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records), 'null_rate': 0.1},
    'ShipmentLineID': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records),
                       'null_rate': 0.1},
    'ReceiptKey': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records), 'null_rate': 0.1},
    'ClientID': {'distribution': lambda num_records: np.random.randint(100, 999, num_records), 'null_rate': 0.1},
    'WarehouseID': {'distribution': lambda num_records: np.random.randint(10, 99, num_records), 'null_rate': 0.1},
    'SiteID': {'distribution': lambda num_records: np.random.randint(10, 99, num_records), 'null_rate': 0.1},
    'ProductID': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records), 'null_rate': 0.1},
    'InventoryStatusID': {'distribution': lambda num_records: np.random.randint(10, 99, num_records), 'null_rate': 0.1},
    'StorageLocationID': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records),
                          'null_rate': 0.1},
    'AssetTypeID': {'distribution': lambda num_records: np.random.randint(1000, 9999, num_records), 'null_rate': 0.1},
    'HoldFlagBool': {'distribution': lambda num_records: np.random.choice([0, 1], num_records), 'null_rate': 0.1},
    'UnitQTY': {'distribution': lambda num_records: np.random.randint(1, 500, num_records), 'null_rate': 0.1},
    'Weight': {'distribution': lambda num_records: np.random.random(num_records) * 1000, 'null_rate': 0.05},
    'Volume': {'distribution': lambda num_records: np.random.random(num_records) * 10, 'null_rate': 0.05},
    'Category': {'distribution': lambda num_records: np.random.choice(["Electronics", "Clothing", "Furniture", "Food"],
                                                                      num_records, p=[0.7, 0.1, 0.1, 0.1]),
                 'null_rate': 0.1},
    'Supplier': {
        'distribution': lambda num_records: np.random.choice(["SupplierA", "SupplierB", "SupplierC"], num_records,
                                                             p=[0.5, 0.3, 0.2]), 'null_rate': 0.1},
    'Status': {'distribution': lambda num_records: np.random.choice(["Pending", "Shipped", "Delivered"], num_records),
               'null_rate': 0.1},
    'Priority': {'distribution': lambda num_records: np.random.choice(["High", "Medium", "Low"], num_records),
                 'null_rate': 0.1},
}

# New DataFrame for summarized data
summary_data = pd.DataFrame(columns=['TableName', 'TimeStamp', 'Creations', 'Updates', 'Deletions', 'HourType', 'DayType'])

# Helper function to generate a timestamp within a specific hour
def random_timestamp_within_hour(base_date):
    minute = np.random.randint(0, 60)
    second = np.random.randint(0, 60)
    return base_date + timedelta(minutes=minute, seconds=second)

# Function to generate records
def create_records(num_records, current_datetime):
    global current_id_index
    new_records = {column: CONFIG[column]['distribution'](num_records) for column in CONFIG}
    new_records['ID'] = new_ids[current_id_index:current_id_index + num_records]
    current_id_index += num_records
    new_records['Created'] = [random_timestamp_within_hour(current_datetime) for _ in range(num_records)]
    new_records['Modified'] = new_records['Created']
    new_records['isDeleted'] = [False] * num_records
    new_records['Day'] = [current_datetime.strftime('%Y-%m-%d')] * num_records  # Populate the Day column
    new_records['Hour'] = [current_datetime.hour] * num_records  # Populate the Hour column
    return pd.DataFrame(new_records)

# Function to update records
def update_records(df, num_updates, current_datetime):
    for _ in range(num_updates):
        # Select a random record that is not deleted
        candidates = df[df['isDeleted'] == False]
        if candidates.empty:
            break
        idx = np.random.choice(candidates.index)
        column_to_update = np.random.choice(['StorageLocationID', 'InventoryStatusID', 'Status', 'Priority'])
        new_value = np.random.randint(1000, 9999) if column_to_update == 'StorageLocationID' else np.random.choice(
            ["Pending", "Shipped", "Delivered", "High", "Medium", "Low"])
        df.loc[idx, column_to_update] = new_value
        df.loc[idx, 'Modified'] = random_timestamp_within_hour(current_datetime)
        df.loc[idx, 'Day'] = current_datetime.strftime('%Y-%m-%d')  # Update the Day column
        df.loc[idx, 'Hour'] = current_datetime.hour  # Update the Hour column

# Function to delete records
def delete_records(df, num_deletes, current_datetime):
    for _ in range(num_deletes):
        # Select a random record that is not deleted
        candidates = df[df['isDeleted'] == False]
        if candidates.empty:
            break
        idx = np.random.choice(candidates.index)
        df.loc[idx, 'isDeleted'] = True
        df.loc[idx, 'Modified'] = random_timestamp_within_hour(current_datetime)
        df.loc[idx, 'Day'] = current_datetime.strftime('%Y-%m-%d')  # Update the Day column
        df.loc[idx, 'Hour'] = current_datetime.hour  # Update the Hour column

# Ensure the correct data types for columns before concatenation
def ensure_consistent_types(df):
    df['ID'] = df['ID'].astype(int)
    df['Created'] = pd.to_datetime(df['Created'])
    df['Modified'] = pd.to_datetime(df['Modified'])
    df['isDeleted'] = df['isDeleted'].astype(bool)
    df['Day'] = pd.to_datetime(df['Day']).dt.date
    df['Hour'] = df['Hour'].astype(int)
    return df

# Start timing the entire process
start_time = time.time()

# Iterate through the days and hours
current_datetime = start_datetime
for day in range(num_days):
    day_start_time = time.time()

    for hour in range(total_hours):
        num_creations, num_updates, num_deletes = 0, 0, 0
        hour_type = "Active" if active_hours_start <= current_datetime.hour < inactive_hours_start else "Inactive"
        day_type = "Weekend" if current_datetime.weekday() >= 5 else "Weekday"

        if day_type == "Weekday":
            if hour_type == "Active":
                num_creations = max(0, int(np.random.normal(350, 65)))
                num_updates = max(0, int(np.random.normal(80, 20)))
                num_deletes = max(0, int(np.random.normal(30, 15)))
            else:
                num_creations = max(0, int(np.random.normal(70, 20)))
                num_updates = max(0, int(np.random.normal(30, 5)))
                num_deletes = max(0, int(np.random.normal(10, 5)))
        else:  # Weekend
            if hour_type == "Active":
                num_creations = max(0, int(np.random.normal(250, 45)))
                num_updates = max(0, int(np.random.normal(60, 15)))
                num_deletes = max(0, int(np.random.normal(20, 10)))
            else:
                num_creations = max(0, int(np.random.normal(50, 15)))
                num_updates = max(0, int(np.random.normal(20, 5)))
                num_deletes = max(0, int(np.random.normal(5, 3)))

        # Create records
        new_records_df = create_records(num_creations, current_datetime)
        df = pd.concat([df, ensure_consistent_types(new_records_df)], ignore_index=True)

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
            'HourType': hour_type,
            'DayType': day_type
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
