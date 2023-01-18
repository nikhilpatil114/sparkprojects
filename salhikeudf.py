import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data="E:\\bigdatafile\\salhikeudf.csv"
df=spark.read.format('csv').option("header","true").option('inferSchema','true').load(data)
df.show()
df.createOrReplaceTempView("tab")
def func(a,b):
    totalhike=a+b
    return totalhike
res=udf(func)
#df1=df.withColumn("new sal",res("sal","hike"))
#df1.show()
spark.udf.register("hikes",res) #user define function convert to sql function
ndf=spark.sql("select *,hikes(sal,hike)as newsal from tab")
ndf.show()