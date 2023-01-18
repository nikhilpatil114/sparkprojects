import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data="E:\\bigdatafile\\zomato-restaurants-data\\file1.json"
df=spark.read.format('json').option("multiline","true").option('inferSchema','true').load(data)
df.show(20)
df.printSchema()


l1=[]
for i in df.schema.fields:
    if (type(i.dataType) == ArrayType) :
        l1.append(i.name)
for i in range(len(l1)):
    df=df.withColumn(l1[i],explode(l1[i]))
df.show()
df.printSchema()



'''


def func(df):
         complex_data=dict([(field.name,field.dataType)
                  for field in df.schema.fields
                  if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
         while len(complex_data)!=0:
              col_name=list(complex_data.keys())[0]
              if (type(complex_data[col_name])==StructType):
                     expand=(col(col_name +'.'+k).alias(col_name+'_'+k) for k in
                     (n.name for n in complex_data[col_name]))
                     df=df.select('*',*expand).drop(col_name)
              elif (type(complex_data[col_name]) == ArrayType):
                     df=df.withColumn(col_name,explode_outer(col_name))

              complex_data = dict([(field.name, field.dataType)
                            for field in df.schema.fields
                            if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

         return df
my_df=func(df)
my_df.show()
my_df.printSchema()

'''






