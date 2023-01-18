from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").config("org.apache.spark.SparkException","false").getOrCreate()
Access_key_ID="AKIA5GDHIFJUAKLJ337T"
Secret_access_key="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"
# Enable hadoop s3a settings
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",Access_key_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",Secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

data="s3a://nik114//donation.csv"
op="s3a://nik114//donnew1"
df=spark.read.format('csv').option("header","true").option("inferSchema","true").load(data)
df.show()
df.write.format("csv").mode("overwrite").save(op)
