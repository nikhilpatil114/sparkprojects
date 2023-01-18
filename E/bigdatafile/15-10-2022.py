import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E://bigdatafile/qote.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.show()
for i in df.columns:
    df=df.withColumn(i,regexp_replace(i,"'",""))
df.withColumn("email_host",instr("email","@")).show()




'''
colm=[]
win=Window.partitionBy(col("id")).orderBy(col("date").desc())
for i in df.columns:
    colm.append(i.strip())
print(colm)
df=df.toDF(*colm)
df.groupBy("id").agg({"date":"max"}).show()
#df.withColumn("drn",dense_rank().over(win)).filter(col("drn")==2).show()
#df1=df.withColumn("utime",unix_timestamp()).withColumn("timest",current_timestamp())
#df1.withColumn("datenew",to_date(from_unixtime("utime"))).show()
#df1.withColumn("datenew",to_date(to_timestamp("utime"))).show()
'''