from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
raw_df = spark.readStream \
        .format("json").option("multiLine","true") \
        .option("path", "E:\\nifi-1.17.0\\livedata") \
        .option("maxFilesPerTrigger", 1) \
        .load()


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

df=flatten(raw_df)

df.printSchema()


def foreach_batch_function(df, epoch_id):
     from configparser import ConfigParser
     conf = ConfigParser()
     conf.read(r"E:\bigdatafile\\confi.txt")
     host = conf.get("cred", "host")
     user = conf.get("cred", "user")
     pwd = conf.get("cred", "pass")
     df.write.mode("append").format("jdbc").option("url", host).option("user", user) \
            .option("password", pwd).option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "nifikafalog").save()

pass


df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()

'''    df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
'''