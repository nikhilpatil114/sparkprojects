###AWS LAMBDA PROGRAM FOR PASS THE ARGUMENT TO THE GLUE JOB


import json
import boto3

def lambda_handler(event, context):
   # Retrieve File Information
   bucket_name =   event['Records'][0]['s3']['bucket']['name']
   s3_file_name =  event['Records'][0]['s3']['object']['key']
   client = boto3.client('glue')
   print("bucket: ",bucket_name)
   print("file: ", s3_file_name)
   response = client.start_job_run(JobName = 'lmdajob', Arguments={"--buck":bucket_name,"--file":s3_file_name})

####AWS GLUE PROGRAM

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
filename=file_name[4:]
input_file_path="s3a://{}/{}".format(bucket_name,file_name)
output="s3a://{}/{}".format(bucket_name,"/output/"+filename)
print("Input File Path : ",input_file_path);

df = spark.read.format("csv").option("header", True).option("inferSchema", False).load(input_file_path)
df.write.mode('overwrite').format("jdbc").option("url","jdbc:mysql://mysqldb.cieueozsgk1q.ap-south-1.rds.amazonaws.com:3306/ram").option("user","nikhilpatil").option("password","nikhilpatil").option("dbtable","glutab").option("driver","com.mysql.jdbc.Driver").save()
#df.write.mode('overwrite').format("csv").option("header", "true").save(output)
NW_PROVIDER = ["airtel","vodafone","vodafone in","Airtel","VODAFONE","AIRTEL","VODAFONE IN","Idea","VI","other","OTHERS"]