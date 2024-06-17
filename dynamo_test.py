import boto3
from boto3.dynamodb.conditions import Key
from davinci.services.auth import get_secret

# Initialize a session using Amazon DynamoDB


boto3_login = {
        "verify": False,
        "service_name": 'dynamodb',
        "region_name": 'us-east-1',
        "aws_access_key_id": get_secret("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": get_secret("AWS_SECRET_ACCESS_KEY")
    }
dynamodb = boto3.resource(**boto3_login)



# Select your DynamoDB table
table = dynamodb.Table('data_observability_test')

# Define the sort key value
sort_key_value = "202406051200"

# Query the table
response = table.query(
    KeyConditionExpression=Key('key').eq('data#table#X#column#y#metric#volume') & Key('datetime').gt(sort_key_value)
)

# Print the items
items = response['Items']
for item in items:
    print(item)