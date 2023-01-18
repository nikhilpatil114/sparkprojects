import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E://bigdatafile/null.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.filter((col("name").isNull()) & (col("sal").isNull())).agg(count('*').alias("counts")).show()
#df.dropna(subset=["name","sal"],thresh=2).show()
