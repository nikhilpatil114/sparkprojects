from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data="E:\\bigdatafile\\employees.csv"
df=spark.read.format("csv").option("header",True).option("inferSchema",True).load(data)
df.show()

