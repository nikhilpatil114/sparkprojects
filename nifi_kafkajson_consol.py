from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import re

spark = SparkSession \
    .builder \
    .appName("File Streaming Demo") \
    .master("local[*]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.extraClassPath", "C:\\spark\\spark-3.1.2-bin-hadoop3.2\\jars\\*") \
    .config("spark.executor.extraClassPath", "C:\\spark\\spark-3.1.2-bin-hadoop3.2\\jars\\*") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.debug.maxToStringFields", "200") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sep15") \
    .option("startingOffsets", "earliest") \
    .load()
res = kafka_df.selectExpr("CAST(value AS STRING)")
sch = spark.read.format("json").option("multiLine", "true").load("E:\\nifi-1.17.0\\livedata").schema
fdf = res.withColumn("value", from_json(col("value"), sch))
fdf.printSchema()


fdf.writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()
def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df;


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    cols = [re.sub('[^a-zA-Z0-1]', "", c.lower()) for c in df.columns]
    return df.toDF(*cols);


df1 = flatten(fdf)

df1.printSchema()
res.writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()
def foreach_batch_function(df, epoch_id):
    from configparser import ConfigParser
    conf = ConfigParser()
    conf.read(r"E:\bigdatafile\\confi.txt")
    host = conf.get("cred", "host")
    user = conf.get("cred", "user")
    pwd = conf.get("cred", "pass")
    df.write.mode("append").format("jdbc").option("url", host).option("user", user) \
        .option("password", pwd).option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "nifijson").save()



pass

df1.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()

'''res.writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()
'''