import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
from pyspark.sql.functions import *
sc=spark.sparkContext
'''
data2="E:\\prac_file\\samp1.txt"
rdd1=sc.textFile(data2)
frame='ENO string,NAME STRING,email STRING'
df=rdd1.map(lambda x:x.split(":")).toDF(frame)


df2=df.withColumn("eno",col("eno").cast("Integer"))
df2.show()
df2.printSchema()
df2.select(df2.eno).show()


sc=spark.sparkContext
#data=[12,32,34,4,54,26]
#drdd=spark.sparkContext.parallelize(data)
'''
data="C:\\Driver\\drivers\\asl.csv"
aslrdd=sc.textFile(data)
frame='name STRING,age string,city STRING'
df=aslrdd.map(lambda x:x.split(",")).toDF(frame)
df.show()
#filter by default apply a logic /filter on top of entire line
#filter almost in sql ur using where condition to filter results similarly ur using filter function to filter values.
'''
for i in res.collect():
    print(i)
'''