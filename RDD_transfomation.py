from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
#rdd=spark.sparkContext.parallelize(range(10))
data="E:\\bigdatafile\\donation.csv"
rdd=spark.sparkContext.textFile(data)
#Spark Transformation
rdd0=rdd.coalesce(1)
print(rdd0.getNumPartitions())
rdd1=rdd0.repartition(4)
print(rdd1.getNumPartitions())
#rdd2=rdd1.map(lambda x:x*2)
#rdd2=rdd1.map(lambda x:x.split(","))
#rdd2=rdd1.flatMap(lambda x: x.split(","))
'''
#Flatmap
rdd2=rdd1.flatMap(lambda x: x.split(","))
for element in rdd2.collect():
    print(element)
#map
rdd3=rdd2.map(lambda x: (x,1))
for element in rdd3.collect():
    print(element)
#reduceByKey
rdd4=rdd3.reduceByKey(lambda a,b: a+b)
for element in rdd4.collect():
    print(element)
#map
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
for element in rdd5.collect():
    print(element)
#filter
rdd6 = rdd5.filter(lambda x : 'e' in x[1])
for element in rdd6.collect():
    print(element)
print(rdd2.collect())
'''