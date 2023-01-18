import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
ex=re.sub
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E://bigdatafile//sal1.txt"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.createOrReplaceTempView("mytab")
df.show()
spark.sql("select * from mytab where name like '%am%'").show()
#df.show()
df1=df.withColumn("sal",col("sal") + 1000)
#print(df1.count())
#df.agg(sum("sal")).show()
data=[[1,2,3,4],[5,6,7]]
rdd=spark.sparkContext.parallelize(data)
rdd2=rdd.flatMap(lambda x:x)
#print(rdd2.collect())

