import json
import boto3
import io
import pandas as pd
import s3fs
import fsspec
import csv

s3 = boto3.client('s3')


# import sys

def lambda_handler(event, context):
    # Retrieve File Information
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    input_file_path = "s3://{}/{}".format(bucket_name, s3_file_name)
    file=s3_file_name[5:]
    print(input_file_path)
    df = pd.read_csv(input_file_path,index_col=None)
    print(df)
    output = "s3a://{}/{}".format(bucket_name, "output/"+file)
    df.to_csv(output)
    print("load")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
# response = client.start_job_run(JobName = 'lambdaGluePOC', Arguments={"--buck":bucket_name,"--file":s3_file_name})
