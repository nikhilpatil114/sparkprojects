from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data="E:\\bigdatafile\\detail.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("escape","\"").load(data)
#rdd=rdd.filter(lambda x: "name" not in x).flatMap(lambda x:x.split(",")).map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y))
#sch=StructType([StructField("name",StringType(),True),StructField("address",ArrayType(StringType()),True)])
df.withColumn("address",regexp_replace("address",r'[\W+]',"")).withColumn("address",regexp_replace("address",r'\]',"")).show()
df.printSchema()

"""
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])"""






















'''
import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data="E:\\bigdatafile\\us-500.csv"
rdd=spark.sparkContext.textFile(data)
#rdd2=rdd.map(lambda x:x.replace("\"",''))
df=spark.read.csv(rdd,header=True,sep=',',inferSchema=True)

df2=df.withColumn('phone1',regexp_replace('phone1','-','')).withColumn('ename',substring_index(col('email'),'@',1))\
        .withColumn('mail',substring_index(col('email'),'@',-1))
import re
colm=[re.sub('_','-',k.upper()) for k in df2.columns]
#df2.show()
#lst=[]
#for k in df.columns:
#     lst.append('my'+'_'+k)
#print(lst)
df3=df2.toDF(*colm)
#df3.show()
#df3.printSchema()
#df3.show()
c=[]
for i in df3.columns:
    c.append(i)

for j in c:
     df3=df3.withColumn(j,regexp_replace(j,' ',''))
df3.show()
'''