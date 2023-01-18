# lambda function
import json
import boto3

def lambda_handler(event, context):
   # Retrieve File Information
   bucket_name =   event['Records'][0]['s3']['bucket']['name']
   s3_file_name =  event['Records'][0]['s3']['object']['key']
   client = boto3.client('glue')
   print("bucket: ",bucket_name)
   print("file: ", s3_file_name)
   response = client.start_job_run(JobName = 'lambdaGluePOC', Arguments={"--buck":bucket_name,"--file":s3_file_name})


#### spark script
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["file","buck"])
file_name=args['file']
bucket_name=args['buck']
print("Bucket Name" , bucket_name)
print("File Name" , file_name)
input_file_path="s3a://{}/{}".format(bucket_name,file_name)
output="s3a://{}/{}".format(bucket_name,"/output/aslres")
print("Input File Path : ",input_file_path);

df = spark.read.format("csv").option("header", True).option("inferSchema", False).load(input_file_path)
df.coalesce(1).write.format("csv").option("header", "true").save(output)


'''import json
import boto3
import io
import pandas as pd
import s3fs
import fsspec
import csv

def lambda_handler(event, context):
    # TODO implement
    print(1)
    s3 = boto3.client('s3')
    
    #obj = s3.get_object(Bucket='anurag2025',Key='data/info.txt')
    #df=pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    df=pd.read_csv('s3://anurag2025/data/info.txt')
    print(df)
    
    #bucket=s3.Bucket('anurag2025')
    #with open('s3://anurag2025/data/', 'r') as file:
    #    my_reader = csv.reader(file, delimiter=',')
    #    for row in my_reader:
    #        print(row)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }'''