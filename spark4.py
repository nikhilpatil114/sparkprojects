import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()

# importing all from pyspark.sql.function
import pyspark

from pyspark.sql import *
from pyspark.sql.functions import *

# creating SparkSession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
#
#data="C:\\BigData\\datasets-20220727T020123Z-001\\datasets\\donations.csv"
#df=spark.read.format("csv").option("header","true").load(data)
#if u mention header true, first line of data consider as columns
#if u have amy mal records like first ine second line wrong clean that data using rdd or udf.
#skip first line second line onwards original data available.
data="C:\\BigData\\datasets-20220727T020123Z-001\\datasets\\donations1.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata= rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)
df.printSchema()
#printing columns and its datatype in nice tree format.
df.show(5)
#display top 20 rows by default.if u want to display top 5 lines use show(5)


# importing all from pyspark.sql.function
import pyspark

from pyspark.sql import *
from pyspark.sql.functions import *

# creating SparkSession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
data="C:\\BigData\\datasets-20220727T020123Z-001\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
#sep option used to specify delimiter
#by default spark every field consider as string, but i want to change columns appropriate datatype like int, double, string, use inforschema, true option
#if u not mention like this 1000+1000 ... if int .. 2000 if string, u ll get 10001000

#data processing programming friendly
#res=df.where(col("age")>90)
#res=df.select(col("age"),col("marital"), col("balance")).where((col("age")>60) & (col("marital")!="married"))
#res=df.where((col("age")>60) | (col("marital")=="married") & (col("balance")>=40000))
#res=df.where(((col("age")>60) | (col("marital")=="married")) & (col("balance")>=40000))
#res=df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
#res=df.groupBy(col("marital")).count()
res=df.groupBy(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb"))
    #where(col("balance")>avg(col("balance")))


#process sql friendly
df.createOrReplaceTempView("tab")
#createOrReplaceTempView .. register this dataframe as a table. its very useful to run sql queries.
#res=spark.sql("select * from tab where age>60 and balance>50000")
#res=spark.sql("select marital, sum(balance) sumbal from tab group by(marital)")
#married, how much balance they have
#divorced , how much balance they have

res.show()
#res.printSchema()


# importing all from pyspark.sql.function
import pyspark

from pyspark.sql import *
from pyspark.sql.functions import *

# creating SparkSession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
data="C:\\BigData\\datasets-20220727T020123Z-001\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
#inferSchema .... when ur readdding data auto convert data to appropriate datatypes means value 4444 converet to int .. 4343.4 convert to double
import re
num = int(df.count())
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
# re .. replace .. except all Small letters, capital letters and number except those any other symbols if u have replace/remove

ndf =df.toDF(*cols)
#toDF used to rename all cloumns , and convert rdd to dataframe ... at that time use toDF

ndf.show(21,truncate=True)
#by default show method showing top 20rows and if any field having more than 20 chars its truncated and shows ...
ndf.printSchema()
#dataframe column names and its datattype dispsplay properly

#data processing programming friendly (datframe api)
res=ndf.groupBy(col("gender")).agg(count(col("*")).alias("cnt"))
res.show()