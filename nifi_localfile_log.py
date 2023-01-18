from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark = SparkSession \
    .builder \
    .appName("File Streaming Demo") \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.extraClassPath", "C:\\spark\\spark-3.1.2-bin-hadoop3.2\\jars\\*") \
    .config("spark.executor.extraClassPath", "C:\\spark\\spark-3.1.2-bin-hadoop3.2\\jars\\*") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sep15") \
    .option("startingOffsets", "earliest") \
    .load()
res = kafka_df.selectExpr("CAST(value AS STRING)")

res.printSchema()

res.writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()