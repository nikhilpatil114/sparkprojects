from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.jars","E:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-snowflake_2.12-2.11.0-spark_3.1").getOrCreate()

sc=spark.sparkContext
sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIA5GDHIFJUAKLJ337T")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")
'''
sfOptions = {
"sfURL" : "mc06057.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "nikhilpatil",
  "sfPassword" : "Nikhil@1494",
  "sfDatabase" : "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema" : "TPCH_SF10",
  "sfWarehouse" : "SMALL"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
#net\snowflake\spark\snowflake
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from ORDERS")\
  .option("autopushdown", "off") \
  .load()

df.show()
#https://docs.snowflake.com/en/user-guide/spark-connector-use.html
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.13.14
#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.11.0-spark_3.1
'''
sfOptions ={
"sfURL" : "mc06057.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "nikhilpatil",
  "sfPassword" : "Nikhil@1494",
  "sfDatabase" : "mydb",
  "sfSchema" : "public",
  "sfWarehouse" : "SMALL"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
data="E:\\bigdatafile\\INFY.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
df.show()
df.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME) \
   .options(**sfOptions) \
   .option('dbtable','INFY')\
   .save()

