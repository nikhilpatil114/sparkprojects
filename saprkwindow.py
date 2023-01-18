import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data="E:\\bigdatafile\\us-500.csv"
rdd=spark.sparkContext.textFile(data)
#rdd2=rdd.map(lambda x:x.replace("\"",''))
df=spark.read.csv(rdd,header=True,sep=',')
res=df.withColumnRenamed('zip','sal')
from pyspark.sql.window import Window
win = Window.partitionBy("state").orderBy(col("sal").desc())
fin = res.drop("county", "company_name",'phone1','phone2','email','web')\
    .withColumn("rnk", rank().over(win))\
    .withColumn("drnk",dense_rank().over(win))\
    .withColumn("rno", row_number().over(win))\
    .withColumn("prank", percent_rank().over(win))\
    .withColumn("ntr", ntile(10).over(win))\
    .withColumn("lead", lead(col("sal")).over(win)).na.fill(0)\
    .withColumn("diff",col("sal") - col("lead"))\
    .withColumn("lag", lag(col("sal")).over(win))\
    .withColumn("fst", first(col("sal")).over(win))

fin.show(50)
#res.createOrReplaceTempView("tab")
#result=res.orderBy(col("sal").desc()).withColumn("rno",monotonically_increasing_id()+1).where(col("rno")<=5)