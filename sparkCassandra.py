import sys
import os
import findspark
findspark.init()
os.environ["JAVA_HOME"]="/usr/lib/jvm/java"
os.environ["SPARK_HOME"]="/usr/lib/spark"
from pyspark.sql import *
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
adf=spark.read.format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","cassdb").load()
adf.show()
edf=spark.read.format("org.apache.spark.sql.cassandra").option("table","emp").option("keyspace","cassdb").load()
edf.show()
#join=adf.join(edf,edf.first_name==adf.name,"fullouter").drop("first_name").na.fill("No Data",["city","name"]).na.fill(0,["id"])
join=adf.join(edf,edf.first_name==adf.name,"fullouter").drop("first_name").na.fill("No Data").na.fill(0)
#inner ... common records, .. leftouter ... get all left side elements but right side u ll get nulls mostly/other unmatched records
#rightouter  get all records from right side and mismatched records ll get nulls

#fullouter ... get all records from left and right and display all records from left and right

#if both dataframe having same column name at that time use join=adf.join(edf,"name","inner")

#by default its inner join ... or use join=adf.join(edf,edf.first_name==adf.name, "inner")
join.show()

join.write.mode("append").format("org.apache.spark.sql.cassandra").option("table","aslempjoin").option("keyspace","cassdb").save()