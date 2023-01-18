from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data="E:\\bigdatafile\\manager.csv"
df=spark.read.format("csv").option("header",True).option("inferSchema",True).load(data)
#df.show()
dfnew=df.alias("df1").join(df.alias("df2"),col("df1.id")==col("df2.manid")).select([col("df1.id"),col("df1.empname"),col("df1.salary").alias("emsal"),col("df1.manid"),col("df2.salary").alias("mansal")]).filter(col("emsal")>col("mansal"))
#dfnew.show()
dfnew.printSchema()
df3=dfnew.withColumn("empsalnew",col("emsal")+1000)
df4=df3.withColumn("hike",when(col("emsal")<30000,"A").when(col("emsal")>30000,"B"))
df4.show()
