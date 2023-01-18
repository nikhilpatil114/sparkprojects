from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
'''
#read data from rdbms and clean and again reload into rdbms
host="jdbc:mysql://mysqldb.cieueozsgk1q.ap-south-1.rds.amazonaws.com:3306/ram?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","nikhilpatil").option("password","nikhilpatil")\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()
#df.show()
#process data
res=df.na.fill(0,['comm','mgr']).withColumn("comm", col("comm").cast(IntegerType()))\
    .withColumn("hiredate", date_format(col("hiredate"),"yyyy/MMM/dd"))

res.write.mode("overwrite").format("jdbc").option("url",host).option("user","nikhilpatil").option("password","nikhilpatil")\
  .option("dbtable","empclean").option("driver","com.mysql.jdbc.Driver").save()

#: java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
#mysql dependency problem so pls add mysql jar and place in spark/jars folder



#config file formation to hide the imp config

[cred]
host =jdbc:mysql://mysqldb.cieueozsgk1q.ap-south-1.rds.amazonaws.com:3306/ram?useSSL=false
user=nikhilpatil
pass=nikhilpatil

[input]
data=E:\bigdatafile\10000Records.csv
opdata=C:\bigdata\datasets-20220727T095200Z-001\datasets\\opdata

[pro]
qry1=select * from tab where
#load all table into hdfs dataframe from rdbms

from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\bigdatafile\\confi.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
#data=conf.get("input","data")
#tabs=['dept','EMP','abc','banktab','DEPT']
qry="(select table_name from information_schema.tables where TABLE_SCHEMA='ram') aaa"
df1=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",qry).option("driver","com.mysql.jdbc.Driver").load()
#for i in df1.collect():
#print(i[0])
tabs=[x[0] for x in df1.collect()]
#host="jdbc:mysql://mysqldb.cieueozsgk1q.ap-south-1.rds.amazonaws.com:3306/ram?useSSL=false"
for i in tabs:
    df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",i).option("driver","com.mysql.jdbc.Driver").load()
    df.show()

#from hdfs load into rdbms table

from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\bigdatafile\\confi.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")
df= spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)
import re

cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)
ndf.show(4)
ndf.printSchema()

ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user",user).option("password",pwd)\
.option("dbtable","mynew").option("driver","com.mysql.jdbc.Driver").save()
'''
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\bigdatafile\\confi.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
#data=conf.get("input","data")
df= spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load("E:\\bigdatafile\\INFY.csv")
import re

cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)
ndf.show(4)
ndf.printSchema()

ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user",user).option("password",pwd)\
.option("dbtable","myinfy").option("driver","com.mysql.jdbc.Driver").save()

"""import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()

from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"E:\bigdatafile\\confi.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
#data=conf.get("input","data")
#tabs=['dept','EMP','abc','banktab','DEPT']
df1=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",'empnew').option("driver","com.mysql.jdbc.Driver").option("partitionColumn","empno").option("numPartitions",3)\
    .option("lowerBound",0).option("upperBound",6).load()
print(df1.count())"""