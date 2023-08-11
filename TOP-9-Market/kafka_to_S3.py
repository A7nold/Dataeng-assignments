import pandas as pd
import boto3 
import merged_dataframes
from consumer import open_records_csv

# merged_dataframes.initiate_database()

# s3_df = merged_dataframes.merged_dfs('open')

#print(consumer_df)

access_key = 'AKIAVBV2DE56S6KCTHPS'
Secret_access_key = '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'
region = 'ap-south-1'

bucket_name = 'top-9-stock-p2'
object_key = 'rawdata/ns/open_record.csv' 

#open_records_csv = s3_df.to_csv(index= False)

s3_client = boto3.client('s3',aws_access_key_id = access_key,aws_secret_access_key = Secret_access_key,region_name = region) 

s3_client.put_object(Bucket = bucket_name,Key = object_key,Body = open_records_csv)


print(f"Data uploaded to S3 bucket: s3://{bucket_name}/{object_key}")