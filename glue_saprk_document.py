#prereqisite definition
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#extract data from crawler
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="yyz-tickets", table_name="tickets", transformation_ctx="S3bucket_node1"
    )
#Transformation

ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("tag_number_masked", "string", "tag_number_masked", "string"),
        ("date_of_infraction", "string", "date_of_infraction", "string"),
        ("ticket_date", "string", "ticket_date", "string"),
        ("ticket_number", "decimal", "ticket_number", "float"),
        ("officer", "decimal", "officer_name", "decimal"),
        ("infraction_code", "decimal", "infraction_code", "decimal"),
        ("infraction_description", "string", "infraction_description", "string"),
        ("set_fine_amount", "decimal", "set_fine_amount", "float"),
        ("time_of_infraction", "decimal", "time_of_infraction", "decimal"),
    ],
    transformation_ctx="ApplyMapping_node2",
)


#load data into target
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://DOC-EXAMPLE-BUCKET", "partitionKeys": []},
    format_options={"compression": "gzip"},
    transformation_ctx="S3bucket_node3",
    )
#Load data from s3 to crawler transform and then load into my sql

df1 = glueContext.create_dynamic_frame.from_catalog(database = "mayur", table_name = "myfd", transformation_ctx = "df1")

df2 = ApplyMapping.apply(frame = df1, mappings = [("name", "string", "name", "string"), ("age", "long", "age", "long"), ("city", "string", "city", "string")], transformation_ctx = "df2")

df3 = ResolveChoice.apply(frame = df2, choice = "make_cols", transformation_ctx = "df3")

df4 = DropNullFields.apply(frame = df3, transformation_ctx = "df4")

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = df4, catalog_connection = "myc", connection_options = {"dbtable": "myfd", "database": "ram"}, transformation_ctx = "df5")
job.commit()

#direct read from s3 using glue
dfsss = glueContext.create_dynamic_frame.from_options(connection_type='s3', connection_options={"paths":["s3://nik114/myfd/asl.csv"]}, format='csv',  transformation_ctx="dfsss",format_options={"withHeader":True})
dfsss.toDF().show()



#from s3 using spark_glue //using from pyspark.sql import DataFrame
dfn=spark.read.format("csv").option("header","true").load("s3://nik114/myfd/asl.csv")
dfn.show()