from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="E:\\bigdatafile\\donation.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
res=df.withColumn("dates",to_date(col("dt"),"dd/MMM/yyyy"))
#res1.show()
#res1.printSchema()
res1=res.withColumn('trundate',date_format(date_add(date_trunc("month",col("dates")).cast(DateType()),14),'dd-MM-yyyy-EE'))\
      .withColumn("today",current_date())
res2=res1.withColumn("dtdiff",datediff(col('today'),col("dates")))
res2.createOrReplaceTempView('tab')
from math import *
def func(n):
    year = floor(n / 365)
    rem = n % 365
    month = floor(rem / 30)
    rem1=rem%30
    day = rem1
    return " {} years {} month {} day ".format(year, month, day)

uf=udf(func)

res2.withColumn('newdiff',uf(col('dtdiff'))).show(truncate=False)
spark.udf.register("newdif",uf)
spark.sql("select name,dt,dtdiff,today,newdif(dtdiff) as newform from tab").show(truncate=False)

#floor use to remove the number after decimal(11.12 ==> 11 or 11.66==>11)
#ceil use to round the decimal number any way(12.40 ==>13 or 12.60==>13)
#round also use to round the decimal number (13.33==>13 or 13.65==>14)

'''
#spark by default able to understand 'yyyy-MM-dd' format only
#but in original data u hve dd-MM-yyyy so this date format convert to spark understandable format.
#to_date convert input date format to 'yyyy-MM-dd' format
#current_dat() used to get today date based on ur system time.
#config("spark.sql.session.timeZone", "EST") ... its very imp based on original client date all default time based on us time only.at that time mention "EST"
#current_timestamp() u ll get seconds minutes as well.

#create udf to get expected date format. like 1yr, 2 months, 4 days ..

res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),100))\
    .withColumn("dtsub",date_sub(col("dt"),100))\
    .withColumn("lastdt",date_format(last_day(col("dt")),"yyyy-MM-dd-EEE"))\
    .withColumn("nxtday",next_day(col("dt"),"Friday"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/zzz"))\
    .withColumn("monLstFri",next_day(date_add(last_day(col("dt")),-7),"Fri"))\
    .withColumn("dayofweek", dayofweek(col("dt")))\
    .withColumn("dayofmon",dayofmonth(col("dt")))\
    .withColumn("dayofyr", dayofyear(col("dt")))\
    .withColumn("monbet" ,months_between(current_date(),col("dt")))\
    .withColumn("floor",floor(col("monbet")))\
    .withColumn("ceil",ceil(col("monbet")))\
    .withColumn("round",round(col("monbet")).cast(IntegerType()))\
    .withColumn("dttrunc",date_trunc("day",col("dt")).cast(DateType()))\
    .withColumn("weekofyear",weekofyear(col("dt")))\
    .withColumn('BirthDate',to_timestamp('BirthDate','yyyy-MM-dd HH:mm:ss.SSSS')) -->convert string timestamp to timestamp data type 


res.printSchema()
res.show(truncate=False)
#dtdiff .. 588 days .. i want to conver to 1yr-3mon-4days ..
#dayofweek ... from sun how many days completed.. if sun ..1, mon.2.tue..3..sat 7
#dayofmon .... from month 1 to how many days completed
#dayofyear .... from jan 1 to specified date, how many days completed.
#date_add(df.dt, -100) and date_sub(df.dt, 100) both are same.
#last_day ... it return month's last day.. let jan lastday jan 31.. feb lastday 28
#whats next sun, next mon, next wednesday from today ull get. next_day(dt, "sun")
#https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
#date_format used to get ur desired format date. let eg: 20/April/21/Tuesday/ at that time use     .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/"))

#tasks: i want udf get dtdiff conver to 3 years, 4 months 9 days
#every month 15th what day u ll get? (sun?or mon)
'''
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data='E:\\bigdatafile\\covid.txt'
df=spark.read.format('csv').option('inferSchema','true').option('header','true').load(data)
df.withColumn('ObservationDate',to_date('ObservationDate','MM/dd/yyyy')).withColumn('Last Update',to_timestamp('Last Update','M/d/yyyy mm:ss')).show()

#both to date and timestamp are possible on the sting data type
df1=df.withColumn("date",to_date("date","dd/MMM/yyyy")).withColumn("timedate",to_timestamp("date","yyyy-MM-dd HH:mm:SSSS"))
df1=df.withColumn("timestamp",to_timestamp("date","dd/MMM/yyyy"))

