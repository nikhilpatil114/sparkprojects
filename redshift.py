from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
host="jdbc:redshift://myredshift.c5cg6bxkkhxw.ap-south-1.redshift.amazonaws.com:5439/dev"
df=spark.read.format("jdbc").option("url",host).option("user","myredshift").option("password","Rpassword.1")\
    .option("dbtable","sales").option("driver","com.amazon.redshift.jdbc.Driver").load()
#df.show()
ndf=df.where(col("commission")>500)
#ndf=df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
ndf.show()
ndf.write.mode("append").format("jdbc").option("url",host).option("user","myredshift").option("password","Rpassword.1")\
    .option("dbtable","sales1").option("driver","com.amazon.redshift.jdbc.Driver").save()

#sql workbench
'''   
create table banktab(age int,job varchar(32),marital varchar(32),
       education varchar(32),default1 varchar(32),balance varchar(32),housing varchar(32),
       loan varchar(32),contact varchar(32),day varchar(32),month varchar(32),
       duration varchar(32),campaign varchar(32),pdays varchar(32),previous varchar(32),
       poutcome varchar(32),y varchar(32)) 
copy banktab from 's3://nik114/mayur/bank-full.csv' iam_role 'arn:aws:iam::906454641256:role/reds3full' csv delimiter ';' 
IGNOREHEADER 1 null as '\000'; 
select * from stl_load_errors  
select * from banktab    
unload ('select * from banktab where age>80')    
to 's3://nik114/mayur/bankresult.csv'  
iam_role 'arn:aws:iam::906454641256:role/reds3full' csv  null as '\000';
# describe table
SELECT * FROM PG_TABLE_DEF WHERE
  schemaname = 'public' and tablename='donation';
  
The most useful object for this task is the PG_TABLE_DEF table, which as the name implies, contains table 
definition information.
The PG_ prefix is just a holdover from PostgreSQL, the database technology from which Amazon Redshift was developed.
'''
"""
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
#Data="E:\\bigdatafile\\donation.csv"
host="jdbc:redshift://myredshift.c5cg6bxkkhxw.ap-south-1.redshift.amazonaws.com:5439/dev"
df=spark.read.format("jdbc").option("url",host).option("user","myredshift")\
    .option("password","Rpassword.1")\
    .option("query","select * from donation1 where dt between '2019-01-01 00:00:00' and '2021-01-31 00:00:00'")\
    .option("driver","com.amazon.redshift.jdbc.Driver").load()

#df1=df.withColumn("dt",to_timestamp("dt","dd/MMM/yyyy")).withColumn("today",current_timestamp())
df.show()
df.printSchema()
#df.write.mode("append").format("jdbc").option("url",host).option("user","myredshift").option("password","Rpassword.1")\
#    .option("dbtable","donation2").option("driver","com.amazon.redshift.jdbc.Driver").save()
"""