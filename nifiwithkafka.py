from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
import re

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType,ArrayType):
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
            if isinstance(df.schema[column_name].dataType,ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    cols = [re.sub('[^a-zA-Z0-1]', "", c.lower()) for c in df.columns]
    return df.toDF(*cols);


df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "mytopic").load()
ndf=df.selectExpr("CAST(value AS STRING)")
res=flatten(ndf)
res.printSchema()
#ndf=df.withColumn("value","CAST(value AS STRING)")
ndf.printSchema()
#query = ndf.writeStream.format("console").start()
#ex=r'^(\S+),(\S+),(\S+)'
#res=ndf.select(regexp_extract('value',ex,1).alias("name"),regexp_extract('value',ex,2).alias("age"),regexp_extract('value',ex,3).alias("city"))
#query = res.writeStream.format("console").start()

def foreach_batch_function(df, bid):
    from configparser import ConfigParser
    conf = ConfigParser()
    conf.read(r"E:\bigdatafile\\confi.txt")
    host = conf.get("cred", "host")
    user = conf.get("cred", "user")
    pwd = conf.get("cred", "pass")
    df.write.mode("append").format("jdbc").option("url",host).option("user",user)\
    .option("password",pwd).option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","nifikafkalive").save()

    pass
#query.awaitTermination()
res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()