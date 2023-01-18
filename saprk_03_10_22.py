import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import functions as f

spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
Data="E:\\bigdatafile\\corona.csv"
df=spark.read.format("csv").option("header",True).option("inferSchema",True).load(Data)
df1=df.withColumn("date",to_date("date","M/d/yyyy"))
#df2=df1.groupby(col("date")).agg(count("date").alias("cnt")).orderBy(col("cnt").desc())
#df2=df1.groupby(col("date")).agg({"death":"sum","recovered":"sum"})
#df2=df1.groupby("Country").pivot("date").agg(sum("confirmed"))
#df2=df1.groupby("country","state_cleaned").agg(max("date").alias("date")).orderBy(col("date").desc())
#df3=df1.join(df2,on=["country","state_cleaned","date"])
#df3=df1.groupby("country","date").agg({"death":"sum","recovered":"sum","confirmed":"sum"}).orderBy("date",ascending=False).filter(col("country")=="India")
#df3=df1.groupby("country").agg(sum("recovered").alias("recovered"),sum("Death").alias("death"),sum("confirmed").alias("confirmed"))\
#    .orderBy("confirmed",ascending=False)
#df3.show()
#df3.withColumn("live",col("confirmed")-(col("death")+col("recovered"))).show()
#df2=df1.select("country","state_cleaned","confirmed","recovered").filter(col("country").isin("Australia","India")).groupBy(col("Country")).sum()
df2=df1.select("country","state_cleaned","confirmed","recovered").filter(col("country").isin("Australia","India")).groupBy(col("Country")).sum()
df2.show()
#print(df3.count())
#df3=df1.withColumn("year",year(col("date")))
#df3.filter(col("year") > 2019).show()
#df2.show()
#df2.printSchema()
df1.printSchema()

#df1=df.select(col("state")).filter(col("state").isNotNull())
#print(df1.count())
# search null present in all column
#df1=df.select([count(when(col(i).isNull(),i)) for i in df.columns])


