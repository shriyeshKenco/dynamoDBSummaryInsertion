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
table = dynamodb.Table(table_name)


def fetch_summary_data(table, days=28):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    start_timestamp = int(start_date.strftime('%Y%m%d%H%M'))
    end_timestamp = int(end_date.strftime('%Y%m%d%H%M'))

    response = table.scan(
        FilterExpression=Key('TimeStamp').between(start_timestamp, end_timestamp)
    )

    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=Key('TimeStamp').between(start_timestamp, end_timestamp),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])

    return pd.DataFrame(data)


def calculate_statistics(df):
    df['Creations'] = df['Creations'].astype(int)
    df['Updates'] = df['Updates'].astype(int)
    df['Deletions'] = df['Deletions'].astype(int)

    stats = df.groupby(['DayType', 'HourType']).agg({
        'Creations': ['mean', 'std'],
        'Updates': ['mean', 'std'],
        'Deletions': ['mean', 'std']
    })
    stats.columns = ['_'.join(col).strip() for col in stats.columns.values]
    return stats


def calculate_thresholds(stats):
    thresholds = {}
    for (day_type, hour_type), group_stats in stats.groupby(level=[0, 1]):
        thresholds[(day_type, hour_type)] = {
            'Creations': {
                'lower': group_stats['Creations_mean'] - 2 * group_stats['Creations_std'],
                'upper': group_stats['Creations_mean'] + 2 * group_stats['Creations_std']
            },
            'Updates': {
                'lower': group_stats['Updates_mean'] - 2 * group_stats['Updates_std'],
                'upper': group_stats['Updates_mean'] + 2 * group_stats['Updates_std']
            },
            'Deletions': {
                'lower': group_stats['Deletions_mean'] - 2 * group_stats['Deletions_std'],
                'upper': group_stats['Deletions_mean'] + 2 * group_stats['Deletions_std']
            }
        }
    return thresholds


def update_thresholds(table):
    df = fetch_summary_data(table)
    if df.empty:
        print("No data found in the specified time range.")
        return None
    stats = calculate_statistics(df)
    thresholds = calculate_thresholds(stats)
    return thresholds


# Function call to update thresholds
thresholds = update_thresholds(table)
print(thresholds)