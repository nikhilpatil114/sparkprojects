from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="E://bigdatafile/wordcount.txt"
text_file =sc.textFile(data)
counts = text_file.flatMap(lambda x:x.split(" ")) \
                            .map(lambda x: (x, 1)) \
                           .reduceByKey(lambda x, y: x + y)
print(counts.collect())