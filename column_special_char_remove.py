import pyspark
from pyspark.sql import udf
from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E:\\bigdatafile\\qote.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
df.show()
def col(d):
    lst=[]
    for i in range(len(d)):
        j=re.sub('[^a-zA-Z0-9\']',"",d[i].lower())
        lst.append(j)
    return lst
res=col(df.columns)


cols=res
df1=df.toDF(*cols)
def func(d):
    for i in d.columns:
        d=d.withColumn(i,regexp_replace(i,"'",''))
    return d
dfnew=func(df1)
dfnew.show()


#df1.show()
#df1.withColumn('name',regexp_replace('name',"'",'')).show(truncate=False)

#df1.groupBy(col('gender')).agg(count('*').alias('cnt')).orderBy(col('cnt').desc()).show()
#df1.createOrReplaceTempView('tab')
#qry="""with A as (select first_name,last_name,year_of_joining,row_number() over(partition by year_of_joining  order by year_of_joining desc) as rn from tab ) select * from A order by year_of_joining desc,rn """
#spark.sql(qry).show()
#qry1="""with tab as (select sales.*,dense_rank() over (partiton by customer order by date desc) as DR from sales) select * from tab where DR=1"""
