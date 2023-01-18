import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data1={(1,"jay"),(2,"mayur"),(3,"ram")}
frame='eno INTEGER,name STRING'
DF=spark.createDataFrame(data=data1,schema=frame)
DF.show()
'''
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data=[1,2,3,4,5]
#convert these nums to rdd .. use parallalise(data)
#sparkContext used to create rdds
#2 ways to create rdds 1) sc.parallalize().. java/scala/python variables/object convert to rdd
# 2)external data (hdfs, local, s3.. a physical data) convert to rdd, sc.textFile()
drdd=sc.parallelize(data)
#what is rdd? collection of jvm objects called rdd, that
# rdd must follow 3 properties immutable, fault tolerance, laziness.


'''


'''
context: a nutshell to create different apis 
sparkContext ...rdd api
sqlContext ....dataframe api
HiveContext ...to connect hive
SparkStreaming context ... to process streaming data
SparkSession Context .... unifying all contexts and all apis. dataset api
abstraction: a fundamental element to do anything called abstraction.
sql ... table, schema (columns) ...
java... JVM object ...
Spark ...Rdd ..
unable to understand 1,2,3,4..if convert 1,2,3,4 to rdd .. spark able to understand
spark able to understand only RDD. spark don't know anything.


3 env
1) learning env ... ubuntu...spark-shell..1%
2) dev & testing env ..windows...pycharm/Eclipse/Intellij..94%
3) prod env .... cloud/linux .....maven/sbt/.py files ..5%
core components.. spark
second party tools..local system .. env .. spark-shell
third party tools.. intellij/pycharm 
'''