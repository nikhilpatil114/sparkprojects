from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdatafile\\zomato-restaurants-data\\file1.json"
df=spark.read.format('json').option("multiline","true").option('inferSchema','true').load(data)
df.printSchema()
def read_nested_json(df):
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    print(column_list)
    df = df.select(column_list)
    #df.show()
    #df.printSchema()
    return df
def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df)
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True

    #cols = [re.sub('[^a-zA-Z0-1]', "_", c.lower()) for c in df.columns]
    return df

ndf=flatten(df)
ndf.show()
ndf.printSchema()










