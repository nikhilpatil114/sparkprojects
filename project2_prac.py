from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="E:\\bigdatafile\\proj2.csv"
df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
df.show()
df.printSchema()
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\bigdatafile\\confi.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
addColDF3 = df.withColumn('create_time', lit(datetime.now()))
#addColDF3.createOrReplaceTempView("classification")
from pyspark.sql.window import Window
win = Window.partitionBy("name","age")


validationDF1=addColDF3.withColumn("error__check",when(trim(col("name")).isNull() | trim(col("age")).isNull() |trim(col("dept")).isNull() | trim(col("city")).isNull() | trim(col("tabno")).isNull() | trim(col("status")).isNull(),'mandatory fileds null')\
                                                  .when(length(trim(col("tabno")).cast(IntegerType()))>5,"failed in length")\
                                                  .when(~lower(trim(col("status"))).isin ("add","update","delete"),"not identofied")\
                                                  .when(count("name").over(win)>1,"duplicate record")
                                                  .otherwise(None)

                                                      )
validationDF1.show(truncate=False)
validationDF1.printSchema()
'''
validationDF = spark.sql("SELECT *, CASE \
                                           WHEN name is null or age is null or dept is null or city is null or \
                                               tabno is null or status is null THEN \
                                              'mandatory fileds null' \
                                          WHEN  length(tabno) > 10  THEN  \
                                              'length check failed' \
                                          WHEN lower(status) not in ('add', 'update', 'delete') THEN\
                                               'unidentified action' \
                                          WHEN COUNT(1) OVER(partition by name) > 1 THEN \
                                               'duplicate records' \
                                          ELSE NULL \
                                     END AS error_desc \
                             FROM  classification")
validationDF.persist()
validationPassDF = validationDF.filter("error_desc IS NULL")
validationFailDF = validationDF.filter("error_desc IS NOT NULL")
validationPassDF.show()
validationFailDF.show()
validationFailDF.write \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url",host) \
    .option("user", user) \
    .option("password", pwd) \
    .option("dbtable", "mayur.error_tablet_detail") \
    .mode("append") \
    .save()

validationPassDF.write \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url",host) \
    .option("user", user) \
    .option("password", pwd) \
    .option("dbtable", "mayur.stage_tablet_detail") \
    .mode("append") \
    .save()


validationDF.unpersist()
'''