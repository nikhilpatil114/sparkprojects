#LISTING S3 BUCKETS
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import boto3
import pandas as pd
import boto3
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
bucket = "nik114"
file_name = "donation.csv"

s3 = boto3.client('s3')
# 's3' is a key word. create connection to S3 using default config and all buckets within S3

obj = s3.get_object(Bucket= bucket, Key= file_name)
# get object and file (key) from bucket
print(obj)

initial_df = pd.read_csv(obj['Body'])
sparkDF=spark.createDataFrame(initial_df)
#print(initial_df)
sparkDF.
pandadf=sparkDF.toPandas()
print(pandadf)