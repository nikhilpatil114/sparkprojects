import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data="E:\\bigdatafile\\zomato-restaurants-data\\file1.json"
df=spark.read.format('json').option("multiline","true").option('inferSchema','true').load(data)
'''
def flatten(df):
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        # print ("processing :"+col_name+" Type: "str(type(complex_fields[col_name])))

        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        elif (type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df
my_frame = flatten(df)
my_frame.show()

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








