import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E://bigdatafile/specha.txt"
df=spark.read.format("csv").option("header",True).option("inferschema",True).load(data)
df1=df.withColumn("timestamp",unix_timestamp())
df1.show()
#df1=df.withColumn("sal",regexp_replace("sal","[^a-zA-Z0-9]",''))
#df1=df.withColumn("sal",translate("sal","@",''))
#df1.withColumn("date",to_date(from_unixtime("timestamp"))).show()
#df2=df1.withColumn("date",from_unixtime("timestamp","yyyy-MM-dd"))
#df1.withColumn("datenew",to_date(from_unixtime("utime"))).show()
#df1.withColumn("datenew",to_date(to_timestamp("utime"))).show()
#df2=df1.withColumn("date",to_date(from_unixtime("timestamp"))).withColumn("date1",date_format("date","yyyy-MM-dd"))
#df.withColumn('address', translate('address', '234', 'DEF'))