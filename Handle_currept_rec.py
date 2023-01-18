from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data='E:\\bigdatafile\\mallrecord.csv'
#df=spark.read.format('csv').option('inferSchema','true').option("mode","DROPMALFORMED").option('header','true').load(data)
sch=spark.read.format("csv").option("header","true").option("inferschema","true").load(data).schema
newsch=sch.add(StructField("wrong",StringType(),True))
df=spark.read.format('csv').option('inferSchema','true').schema(newsch).option("mode","PERMISSIVE").option("columnNameofCorruptRecord","wrong").option('header','true').load(data)
df.cache()
df.count()
df.show()
validDF=df.filter(col("wrong").isNull())
invalidDF=df.filter(col("wrong").isNotNull())
validDF.show()
invalidDF.show()
