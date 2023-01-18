import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("inc").getOrCreate()
data="E:\\bigdatafile\\inc_data.csv"
df=spark.read.format("csv").option("header",True).option("inferSchema",True).load(data)
df=df.withColumn("load_date",current_timestamp())
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\bigdatafile\\confi.txt")
host=conf.get("cred","host")
usr=conf.get("cred","user")
pwd=conf.get("cred","pass")
df.show()

#df.printSchema()
df_tab=spark.read.format("jdbc").option("url",host).option("user",usr).option("password",pwd).option("dbtable","inctab")\
    .option("driver","com.mysql.jdbc.Driver").load()
#df.dropDuplicates()
df3=df.unionByName(df_tab)
df3.createOrReplaceTempView("inc_tab")
df_uptab=spark.sql("select t1.* from inc_tab  t1 join (select eid,max(load_date) max_date from \
                inc_tab  group by eid) s on t1.eid=s.eid and t1.load_date=s.max_date")

df1=df_uptab.orderBy(col("eid"))
df1.show()
#df_uptab.write.format("jdbc").option("url",host).option("user",usr).option("password",pwd).option("dbtable","inctab")\
#    .option("driver","com.mysql.jdbc.Driver").mode("overwrite").save()
