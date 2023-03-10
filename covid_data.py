'''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job


sc = SparkContext()
glueContext = GlueContext(sc)
saprk = glueContext.spark_session
job = Job(glueContext)
# job.init(args['JOB_NAME'],args)

db_name = 'covid1-data-db'
tb_name = 'read'
s3_write_path = 's3://covid1-data/write'


datasource = glueContext.create_dynamic_frame.from_catalog(database=db_name,table_name=tb_name)

dataframe = datasource.toDF()

dataframe01 = dataframe.drop('last update')

corona_df = dataframe01.dropna(thresh=4)

cleansed_data_df = corona_df.fillna(value = 'na_province_state', subset='province/state')

most_cases_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('confirmed')\
    .select('province/state', 'country/region', col("max(confirmed)").alias("Most_Cases"))\
    .orderBy('max(confirmed)', ascending=False)

most_deaths_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('deaths')\
    .select('province/state', 'country/region', col("max(deaths)").alias("Most_Deaths"))\
    .orderBy('max(deaths)', ascending=False)

most_recoveries_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('recovered')\
    .select('province/state', 'country/region', col("max(recovered)").alias("Most_Recovered"))\
    .orderBy('max(recovered)', ascending=False)


transform1 = DynamicFrame.fromDF(most_cases_province_state_df, glueContext, 'transform1')
transform2 = DynamicFrame.fromDF(most_deaths_province_state_df, glueContext, 'transform2')
transform3 = DynamicFrame.fromDF(most_recoveries_province_state_df, glueContext, 'transform3')

datasink1 = glueContext.write_dynamic_frame.from_options(frame=transform1, connection_type="s3", connection_options={
                                                         "path": s3_write_path+'/most-cases'}, format="parquet", transformation_ctx="datasink1")
datasink2 = glueContext.write_dynamic_frame.from_options(frame=transform2, connection_type="s3", connection_options={
                                                         "path": s3_write_path+'/most-deaths'}, format="parquet", transformation_ctx="datasink2")
datasink3 = glueContext.write_dynamic_frame.from_options(frame=transform3, connection_type="s3", connection_options={
                                                         "path": s3_write_path+'/most-recoveries'}, format="parquet", transformation_ctx="datasink3")

job.commit()
'''

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
data='E:\\bigdatafile\\covid.txt'
df=spark.read.format('csv').option('inferSchema','true').option('header','true').load(data)
#df1=df.na.drop(how='any' or 'all',thresh=None or 1,2,7,subset=('col1','col2')) #drop rows that have less than thresh non-null values
#df.show()
#df.printSchema()
#df.withColumn('ObservationDate',to_date('ObservationDate','MM/dd/yyyy')).withColumn('Last Update',to_timestamp('Last Update','M/d/yyyy mm:ss')).show()

#df2=df.withColumnRenamed('Province/State','state').withColumnRenamed('Country/Region','region')
#df2.groupby(col('state'),col('region')).max('Confirmed')\
#    .select('state','region',col('max(Confirmed)').alias('max_confirmed'))\
#    .orderBy('max_confirmed',ascending=False).show()
df.select(count('*')).show()





