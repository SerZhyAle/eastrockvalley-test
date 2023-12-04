import boto3
import pandas as pd
import sys

from io import StringIO
client = boto3.client('s3')
bucket_name = 'eastrockvalley-test'
object_key = 'games.csv'

csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

col_names = ["GameID",
            "UserID",
            "GameType",
            "PlayDate",
            "Duration"]

df = pd.read_csv(StringIO(csv_string), names=col_names, header=0)

df.info()