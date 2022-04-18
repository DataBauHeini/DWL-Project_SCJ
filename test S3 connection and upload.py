import boto3
from decouple import config
import os
import pathlib
from pprint import pprint

ACCESS_KEY = config('AWS_ACCESS_KEY')
SECRET_KEY = config('AWS_SECRET_KEY')
SESSION_TOKEN = config('AWS_SESSION_TOKEN')

s3 = boto3.resource(
    service_name='s3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN
)

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

bucket_name = 'teambucketscj'
object_name = ['bitcoin_Subreddit_2022-04-03.csv']
file_name = ['avalanche_Subreddit_2022-04-04.csv', 'avax_Subreddit_2022-04-04.csv', 'binance_Subreddit_2022-04-03.csv',
             'cardano_Subreddit_2022-04-03.csv', 'coin_Subreddit_2022-04-03.csv',
             'cryptocurrency_Subreddit_2022-04-04.csv', 'luna_Subreddit_2022-04-03.csv',
             'avalanche_Subreddit_2022-04-04.csv']

response = s3.Bucket(bucket_name).upload_file(file_name, object_name)
pprint(response) # if NONE then ok
