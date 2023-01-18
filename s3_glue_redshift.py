import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from pyspark.sql import SparkSession
sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
#spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["file","buck"])
file_name=args['file']
bucket_name=args['buck']
print("Bucket Name" , bucket_name)
print("File Name" , file_name)
filename=file_name[4:]
input_file_path="s3a://{}/{}".format(bucket_name,file_name)
output="s3a://{}/{}".format(bucket_name,"/output/"+filename)
print("Input File Path : ",input_file_path);
host="jdbc:redshift://myredshift.c5cg6bxkkhxw.ap-south-1.redshift.amazonaws.com:5439/dev"
df = spark.read.format("csv").option("header", True).option("inferSchema", False).load(input_file_path)
df.write.mode("append").format("jdbc").option("url",host).option("user","myredshift").option("password","Rpassword.1")\
    .option("dbtable","gluejob").option("driver","com.amazon.redshift.jdbc.Driver").save()
#df.write.mode('overwrite').format("csv").option("header", "true").save(output)
