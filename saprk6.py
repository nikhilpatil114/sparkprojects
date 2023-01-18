import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E:\\bigdatafile\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
#df.show()
def col(d):
    lst=[]
    for i in range(len(d)):
        if i%2==0:
            j=re.sub('[^a-zA-Z0-9\']',"_",d[i].lower())
            lst.append(j)
        else:
            lst.append(d[i])
    return lst
res=col(df.columns)


cols=res


df1=df.toDF(*cols)
#df1.withColumn('e_mail',regexp_replace('e_mail','@','')).show(truncate=False)
#df1.groupBy(col('gender')).agg(count('*').alias('cnt')).orderBy(col('cnt').desc()).show()
#df1.createOrReplaceTempView('tab')
#qry="""with A as (select first_name,last_name,year_of_joining,row_number() over(partition by year_of_joining  order by year_of_joining desc) as rn from tab ) select * from A order by year_of_joining desc,rn """
#spark.sql(qry).show()