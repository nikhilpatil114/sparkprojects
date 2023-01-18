import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data="E:\\bigdatafile\\employees.csv"
import re
df=spark.read.format('csv').option('header','true').option('InferSchema','true').load(data)
tempList = [] #Edit01
for colm in df.columns:
        new_name = colm.strip()
        #new_name = "".join(new_name.split())
        #print(new_name)
        new_name = new_name.replace('.','') # EDIT
        tempList.append(new_name) #Edit02
#print(tempList) #Just for the sake of it #Edit03
df = df.toDF(*tempList) #Edit04

df1=df
for i in df1.columns:
    df1=df1.withColumn(i,regexp_replace(i,"'",''))
df2=df1.withColumn('BirthDate',to_timestamp('BirthDate','yyyy-MM-dd HH:mm:ss.SSSS'))
df2.show()
df2.printSchema()
#df1.groupBy('City').count().show()
#df.createOrReplaceTempView('tab')
#spark.sql("select Region,count(*) from tab group by Region").show()