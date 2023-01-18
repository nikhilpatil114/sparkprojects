import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data1={(1,"2022-11-23"),(2,"2021-09-18"),(3,"2022-07-20")}
frame='eno INTEGER,name STRING'
DF=spark.createDataFrame(data=data1,schema=frame)
#DF.show()
#DF.select(current_date().alias("current_date")).show(1)
#DF.select(col("name"),date_format(col("name"), "MM-dd-yyyy").alias("date_format"))
#DF.select(col("name"),to_date(col("name"), "yyyy-MM-dd").alias("to_date"))
#DF.select(col("name"),datediff(current_date(),col("name")).alias("datediff"))
#DF.select(col("name"),months_between(current_date(),col("name")).alias("months_between"))
#DF.select(col("input"),
#    trunc(col("input"),"Month").alias("Month_Trunc"),
#    trunc(col("input"),"Year").alias("Month_Year"),
#    trunc(col("input"),"Month").alias("Month_Trunc")
#   ).show()
#DF.select(col("name"),
#   add_months(col("name"),3).alias("add_months"),
#    add_months(col("name"),-3).alias("sub_months"),
#    date_add(col("name"),4).alias("date_add"),
#    date_sub(col("name"),4).alias("date_sub"))
#DF.select(col("name"),
#     year(col("name")).alias("year"),
#     month(col("name")).alias("month"),
#     next_day(col("name"),"Sunday").alias("next_day"),
#     weekofyear(col("name")).alias("weekofyear"))
#DF.select(col("name"),
#     dayofweek(col("name")).alias("dayofweek"),
#    dayofmonth(col("name")).alias("dayofmonth"),
#    dayofyear(col("name")).alias("dayofyear"))
#DF.select(current_timestamp().alias("current_timestamp"))
#DF.select(col("name"),to_timestamp(col("name"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp"))
'''
data=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
df3=spark.createDataFrame(data,["id","input"])

df3.select(col("input"),
    hour(col("input")).alias("hour"),
    minute(col("input")).alias("minute"),
    second(col("input")).alias("second")
  ).show(truncate=False)
'''

#df1.show(truncate=False)
#df1.printSchema()



