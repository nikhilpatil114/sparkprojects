import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()
data="E://bigdatafile/tab.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
for db in df.columns:
    # Creating table list from the dataframe

    tbl_list = df.select(db).rdd.map(lambda x: x[0]).collect()
    #print(db)
    #print(tbl_list)
lst= ["airtel","vodafone","vodafone in"]
tpl=tuple(lst)
dic=dict((tpl))
print(tpl)
frame="colm string,col1 string,col3 string"
dfnew=spark.createDataFrame(data=dic,schema=frame)
dfnew.show()