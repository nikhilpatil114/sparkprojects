import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
db_name="aryadb"
tb_name="maldata"
#data="s3://ayan2020/input/maldata"
#get data from crawler/ table and create dynamic frame
datasource = glueContext.create_dynamic_frame.from_catalog(database=db_name,table_name=tb_name)

#dynamicframe convert to dataframe
df = datasource.toDF()

#data cleaning
#remove special characters from header/schema

cols=[re.sub('[^0-9a-zA-Z]',"",c)for c in df.columns]
ndf=df.toDF(*cols)
#res=ndf.withColumnRenamed("DateofBirth","dob").withColumn("today", current_date()).where(col("Gender")=="F")
res=ndf.withColumn("DateofBirth",to_date(col("DateofBirth"),"M/d/yyyy")).withColumn("today",current_date()).withColumn("diff",datediff(col("DateofBirth"),col("today")))

#convert dataframe to gluecontext/dynamicframe

#fres = DynamicFrame.fromDF(res, glueContext, 'results')
#store data in s3
#glueContext.write_dynamic_frame.from_options(frame=fres, connection_type="s3", connection_options={"path": s3_write_path}, format="csv", transformation_ctx="datasink1")
#store data in mysql
#glueContext.write_dynamic_frame.from_jdbc_conf(frame = fres, catalog_connection = "mysql", connection_options = {"dbtable": "writetomysql", "database": "aryadb"}, transformation_ctx = "datasink4")
glueContext.write_dynamic_frame.from_options(frame = fres, connection_type = "mysql" ,  connection_options = {"url": "jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb", "user": "myuser", "password": "mypassword", "dbtable": "abcdtestingglue"})
job.commit()