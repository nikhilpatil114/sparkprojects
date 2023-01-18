import pyspark
from pyspark.sql import SparkSession

#spark = SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\Driver\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df1=df.withColumn('email1',substring_index('email','@',-1)).withColumn('username',substring_index('email','@',1))
df2=df1.groupBy(col('state')).agg(count('*'),collect_set(col('city')).alias('list of city'))
df2.show(truncate=False)
df2.printSchema()
'''
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
ndf=df.withColumn("fullname", concat_ws("_",df.first_name, df.last_name, df.state))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))\
    .withColumn("phone2",regexp_replace(col("phone2"),"-","").cast(LongType()))\
    .drop("email","web","city","country","address")\
    .withColumnRenamed("first_name","fname").withColumnRenamed("last_name","lname")

#withColumnRenamed used to rename onecolumn at a time
#withColumn used to add a new column (if column not exists) or update (if already column exists)
#lit(value) used to add something dummy value
#drop (columns) ... delete unnecessary clumns.
ndf.show()
ndf.printSchema()


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\spark venu sir\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#ndf=df.groupBy(df.state).agg(count(col("city")).alias("cnt"), collect_set(df.city).alias("names")).orderBy(col("cnt").desc())
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt"), collect_list(df.first_name).alias("names")).orderBy(col("cnt").desc())

#ndf=df.withColumn("state",when(col("state")=="NY","NewYork").when(col("state")=="CA","Cali").otherwise(col("state")))
#ndf=df.withColumn("address1", when(col("address").contains("#"),"*****").otherwise(col("address"))).withColumn("address2",regexp_replace(col("address"),"#","_"))
#ndf1=df.withColumn("substr",substring(col("email"),0,5)).withColumn("emails", substring_index(col("email"),"@",-1)).withColumn("username", substring_index(col("email"),"@",1))#
#ndf=ndf1.groupBy(col("emails")).count().orderBy(col("count").desc())
df.createOrReplaceTempView("tab")
qry="""with tmp as (select *, concat_ws('_', first_name, last_name) fullname, substring_index(email,'@',-1) mail from tab)
select mail, count(*) cnt from tmp group by mail order by cnt desc
"""
#ndf=spark.sql(qry)
#create ur own functions
def func(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="CA"):
        return "40% off"
    elif(st=="OH"):
        return "50% off"
    else:
        return "500/- off"

#by default spark unable to understand python functions. so convert python/scala/java function to UDF (spark able to understand udfs)
uf = udf(func)
spark.udf.register("offer",uf) #user define function convert to sql function
ndf=spark.sql("select *, offer(state) todayoffers from tab")
#ndf=df.withColumn("offer",uf(col("state")))
ndf.printSchema()

ndf.show(truncate=False)
'''