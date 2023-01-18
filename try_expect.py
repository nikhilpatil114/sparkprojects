import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
path="E:\\bigdatafile\\sales.csv"
df=spark.read.format("csv").option("inferSchema","true").option("header","true").load(path)
#df.show()
df1=df.withColumn("newval",col("cost")-(col("cost")*(col("discount")/100)))
#df1.show()
df.createOrReplaceTempView("tab")
df3=spark.sql("select tab.*,cost-(cost *(discount/100))as newcost from tab")
#df3.show()
#df4=spark.sql("select count(*) as disgot10 from tab where discount=10")
#df4.show()

people = {3: "Jim", 2: "Jack", 4: "Jane", 1: "Jill"}


 # Sort by key
#print(dict(sorted(people.items())))
{1: 'Jill', 2: 'Jack', 3: 'Jim', 4: 'Jane'}

 # Sort by value
#print(dict(sorted(people.items(), key=lambda item: item[1])))
{2: 'Jack', 4: 'Jane', 1: 'Jill', 3: 'Jim'}

Car = {'Audi':100, 'BMW':1292, 'Jaguar': 210000, 'Hyundai' : 88}
lst=Car.values()
print(lst)