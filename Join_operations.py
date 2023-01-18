import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
ex=re.sub
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E://bigdatafile/eid_mid.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df2=df.alias("df2")
df1=df.alias("df3").join(df.alias('df2'),col("df3.id")==col("df2.mid")).select([col("df2.ename"),col("df3.ename").alias("mname")]).orderBy(col("df2.ename"))
df1.show()
#df1=df.alias("df3").join(df.alias('df2'),col("df3.mid")==col("df2.id")).select([col("df3.ename"),col("df2.ename").alias("mname")])
#df1.show()
#spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
#data="E://bigdatafile/eid.csv"
#data1="E://bigdatafile/dept.csv"
#data2="E://bigdatafile/cid.csv"
#df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df2=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1)
#df3=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data2)
#df1.show()
#df2.show()
#df3=df1.join(broadcast(df2),df1.deptno==df2.deptno,"left")
#df4=df1.join(broadcast(df2),df1.deptno==df2.deptno,"right")
#df3=df1.join(broadcast(df2),df1.deptno==df2.deptno,"outer")
#df3=df1.join(broadcast(df2),df1.deptno==df2.deptno,"leftanti")
#df3=df1.join(broadcast(df2),df1.deptno==df2.deptno,"full")
#df3=df1.join(broadcast(df2),df1.deptno==df2.deptno,"leftsemi")
#df3=df1.join(broadcast(df2),df1.deptno==df2.deptno,"rightouter")
#dfnew=df1.join(df2,df1.deptno==df2.deptno).join(df3,df2.cid==df3.cid)
#dfnew.show()
"""'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right',
 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'."""
