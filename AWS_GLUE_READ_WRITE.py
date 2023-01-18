# Example: Read CSV from S3
# For show, we handle a CSV with a header row.  Set the withHeader option.
# Consider whether optimizePerformance is right for your workflow.

from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

dynamicFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://s3path"]},
    format="csv",
    format_options={
        "withHeader": True,
        # "optimizePerformance": True,
    },
)

#######################################################
txId = glueContext.start_transaction(read_only=False)
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = db,
    table_name = tbl,
    transformation_ctx = "datasource0",
    additional_options={
    "transactionId":txId,
    "query":"SELECT * from tblName WHERE partitionKey=value;"
    })
...
glueContext.commit_transaction(txId)

###########################################3
txId = glueContext.start_transaction(read_only=False)
glueContext.write_dynamic_frame.from_catalog(
    frame=dyf,
    database = db,
    table_name = tbl,
    transformation_ctx = "datasource0",
    additional_options={"transactionId":txId})
...
glueContext.commit_transaction(txId)

###################################
glueContext.create_dynamic_frame.from_catalog(
    database="database-name",
    table_name="table-name",
    redshift_tmp_dir=args["TempDir"],
    additional_options={"aws_iam_role": "arn:aws:iam::account-id:role/role-name"})

glueContext.write_dynamic_frame.from_catalog(
    database = "database-name",
    table_name = "table-name",
    redshift_tmp_dir = args["TempDir"],
    additional_options = {"aws_iam_role": "arn:aws:iam::account-id:role/role-name"}) # PySpark For AWS Glue

###########################
# Read from the customers table in the glue data catalog using a dynamic frame
    dynamicFrameCustomers = glueContext.create_dynamic_frame.from_catalog(
    database = "pyspark_tutorial_db",
    table_name = "customers"
    )
# Show the top 10 rows from the dyanmic dataframe
    dynamicFrameCustomers.show(10)


#7. Check data types in Dynamic Frame

# Check types in dynamic frame
    dynamicFrameCustomers.printSchema()


#8. Count The Number of Rows in a Dyanmic DataFrame

# Count The Number of Rows in a Dyanmic Dataframe
    dynamicFrameCustomers.count()


#9. Select Fields From A Dynamic frame

# Selecting certain fields from a Dyanmic DataFrame
    dyfCustomerSelectFields = dynamicFrameCustomers.select_fields(["customerid", "fullname"])

# Show top 10
    dyfCustomerSelectFields.show(10)


#10. Drop Columns in a Dynamic Frame

#Drop Fields of Dynamic Frame
    dyfCustomerDropFields = dynamicFrameCustomers.drop_fields(["firstname","lastname"])

# Show Top 10 rows of dyfCustomerDropFields Dynamic Frame
    dyfCustomerDropFields.show(10)

#11. Rename Columns in a Dynamic Frame

# Mapping array for column rename fullname -> name
    mapping=[("customerid", "long", "customerid","long"),("fullname", "string", "name", "string")]

# Apply the mapping to rename fullname -> name
    dfyMapping = ApplyMapping.apply(
                                    frame = dyfCustomerDropFields,
                                    mappings = mapping,
                                    transformation_ctx = "applymapping1"
                                    )

# show the new dynamic frame with name column
    dfyMapping.show(10)


#12. Filter data in a Dynamic Frame

# Filter dynamicFrameCustomers for customers who have the last name Adams
    dyfFilter=  Filter.apply(frame = dynamicFrameCustomers,
                                            f = lambda x: x["lastname"] in "Adams"
                                        )

# Show the top 10 customers  from the filtered Dynamic frame
    dyfFilter.show(10)


#13. Join Two Dynamic frames on a equality join
#read up orders dynamic frame

# Read from the ccustomers table in the glue data catalog using a dynamic frame
        dynamicFrameOrders = glueContext.create_dynamic_frame.from_catalog(
        database = "pyspark_tutorial_db",
        table_name = "orders"
        )

# show top 10 rows of orders table
        dynamicFrameOrders.show(10)

#join customers and orders dynamic frame

# Join wwo dynamic frames on an equality join
        dfyjoin = dynamicFrameCustomers.join(["customerid"],["customerid"],dynamicFrameOrders)

# show top 10 rows for the joined dynamic
        dfyjoin.show(10)


#14. Write Down Data from a Dynamic Frame To S3
#Create a folder in the S3 bucket created by Cloudformation to use as a location to write the data down to `write_down_dyf_to_s3`
#Write down data to S3 using the dyanmic DataFrame writer class for an S3 path.
        ```
# write down the data in a Dynamic Frame to S3 location.
        glueContext.write_dynamic_frame.from_options(
                                frame = dynamicFrameCustomers,
                                connection_type="s3",
                                connection_options = {"path": "s3://<YOUR_S£_BUCKET_NAME>/write_down_dyf_to_s3"},
                                format = "csv",
                                format_options={
                                    "separator": ","
                                    },
                                transformation_ctx = "datasink2")


15. Write Down Data from a Dynamic Frame using Glue Data Catalog

# write data from the dynamicFrameCustomers to customers_write_dyf table using the meta data stored in the glue data catalog
    glueContext.write_dynamic_frame.from_catalog(
        frame=dynamicFrameCustomers,
        database = "pyspark_tutorial_db",
        table_name = "customers_write_dyf"
    )


#16. Convert from Dynamic Frame To Spark DataFrame

# Dynamic Frame to Spark DataFrame
    sparkDf = dynamicFrameCustomers.toDF()

#show spark DF
    sparkDf.show()


#17. Selecting Colunmns In a Spark DataFrame

# Select columns from spark dataframe
    dfSelect = sparkDf.select("customerid","fullname")

# show selected
    dfSelect.show()

#18. Add Columns In A Spark Dataframe
#creating a new column with a literal string

#import lit from sql functions
    from  pyspark.sql.functions import lit

# Add new column to spark dataframe
    dfNewColumn = sparkDf.withColumn("date", lit("2022-07-24"))
# show df with new column
    dfNewColumn.show()

#Using concat to concatonate two columns together

#import concat from functions
    from  pyspark.sql.functions import concat

# create another full name column
    dfNewFullName = sparkDf.withColumn("new_full_name",concat("firstname",concat(lit(' '),"lastname")))

#show full name column
    dfNewFullName.show()

#19. Dropping Columns

# Drop column from spark dataframe
    dfDropCol = sparkDf.drop("firstname","lastname")

#show dropped column df
    dfDropCol.show()

#20. Renaming columns

# Rename column in Spark dataframe
    dfRenameCol = sparkDf.withColumnRenamed("fullname","full_name")

#show renamed column dataframe
    dfRenameCol.show()

#21. GroupBy and Aggregate Operations

# Group by lastname then print counts of lastaname and show
    sparkDf.groupBy("lastname").count().show()

#22. Filtering Columns and Where clauses
#Filter the spark dataframe

# Filter spark DataFrame for customers who have the last name Adams
    sparkDf.filter(sparkDf["lastname"] == "Adams").show()

#Where clause

# Where clause spark DataFrame for customers who have the last name Adams
    sparkDf.where("lastname =='Adams'").show()


#23. Joins
#read up orders dataset and convert to spark dataframe

# Read from the ccustomers table in the glue data catalog using a dynamic frame and convert to spark dataframe
    dfOrders = glueContext.create_dynamic_frame.from_catalog(
                                            database = "pyspark_tutorial_db",
                                            table_name = "orders"
                                        ).toDF()

#Inner join for Spark Dataframe All Data

# Inner Join Customers Spark DF to Orders Spark DF
    sparkDf.join(dfOrders,sparkDf.customerid ==  dfOrders.customerid,"inner").show(truncate=False)

#Inner Join Adams only

#Get customers that only have surname Adams
    dfAdams = sparkDf.where("lastname =='Adams'")

# inner join on Adams DF and orders
    dfAdams.join(dfOrders,dfAdams.customerid ==  dfOrders.customerid,"inner").show()

#Left Join

#left join on orders and adams df
    dfOrders.join(dfAdams,dfAdams.customerid ==  dfOrders.customerid,"left").show(100)


#24. Writing data down using the Glue Data Catalog
#delete data from S3 in `customers_write_dyf` and `write_down_dyf_to_s3`
#convert from spark Dataframe to Glue Dynamic DataFrame

    # Import Dyanmic DataFrame class
    from awsglue.dynamicframe import DynamicFrame

#Convert from Spark Data Frame to Glue Dynamic Frame
    dyfCustomersConvert = DynamicFrame.fromDF(sparkDf, glueContext, "convert")

#Show converted Glue Dynamic Frame
    dyfCustomersConvert.show()



 # write down the data in converted Dynamic Frame to S3 location.
    glueContext.write_dynamic_frame.from_options(
                                frame = dyfCustomersConvert,
                                connection_type="s3",
                                connection_options = {"path": "s3://<YOUR_S3_BUCKET_NAME>/write_down_dyf_to_s3"},
                                format = "csv",
                                format_options={
                                    "separator": ","
                                    },
                                transformation_ctx = "datasink2")



# write data from the converted to customers_write_dyf table using the meta data stored in the glue data catalog
    glueContext.write_dynamic_frame.from_catalog(
        frame = dyfCustomersConvert,
        database = "pyspark_tutorial_db",
        table_name = "customers_write_dyf")


#############################################################################################3
# Spark Redshift connector Example Notebook - PySpark

 jdbcURL = "jdbc:redshift://redshifthost:5439/database?user=username&password=pass"
 tempS3Dir = "s3://path/for/temp/data"

 # Read Redshift table using dataframe apis
 df = spark.read \
       .format("com.qubole.spark.redshift") \
       .option("url", jdbcURL) \
       .option("dbtable", "tbl") \
       .option("forward_spark_s3_credentials", "true")
       .option("tempdir", tempS3Dir) \
       .load()

 # Load Redshift query results in a Spark dataframe
 df = spark.read \
       .format("com.qubole.spark.redshift") \
       .option("url", jdbcURL) \
       .option("query", "select col1, col2 from tbl group by col3") \
       .option("forward_spark_s3_credentials", "true")
       .option("tempdir",tempS3Dir) \
       .load()


 # Create a new redshift table with the given dataframe data
 # df = <dataframe that is to be written to Redshift>
 df.write \
   .format("com.qubole.spark.redshift") \
   .option("url",jdbcURL) \
   .option("dbtable", "tbl_write") \
   .option("forward_spark_s3_credentials", "true")
   .option("tempdir", tempS3Dir) \
   .mode("error") \
   .save()

 // To overwrite data in Redshift table
 df.write \
   .format("com.qubole.spark.redshift") \
   .option("url",jdbcURL) \
   .option("dbtable", "tbl_write") \
   .option("forward_spark_s3_credentials", "true")
   .option("tempdir", tempS3Dir) \
   .mode("overwrite") \
   .save()

 # Using IAM Role based authentication instead of keys
 df.write \
   .format("com.qubole.spark.redshift") \
   .option("url",jdbcURL) \
   .option("dbtable", "tbl") \
   .option("aws_iam_role", <IAM_ROLE_ARN>) \
   .option("tempdir", tempS3Dir) \
   .mode("error") \
   .save()

###############################################
my_conn_options = {
    "url": "jdbc:redshift://host:port/redshift database name",
    "dbtable": "redshift table name",
    "user": "username",
    "password": "password",
    "redshiftTmpDir": args["TempDir"],
    "aws_iam_role": "arn:aws:iam::account id:role/role name"
}

df = glueContext.create_dynamic_frame_from_options("redshift", my_conn_options)

######################################################
df.write.format("jdbc").\
    option("url", "jdbc:redshift://<host>:5439/<database>").\
    option("dbtable", "<table_schema>.<table_name>").\
    option("user", "<username>").\
    option("password", "<password>").\
    mode('overwrite').save()

################################
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# EXTRACT: Reading parquet data

# Tables bases read
connection_options = {
    "url": "jdbc:redshift://<host>:5439/<database>",
    "dbtable": "<table_schema>.<table_name>",
    "user": "<username>",
    "password": "<password>",
    "redshiftTmpDir": args["TempDir"]
}

# Query based read
# query = "select * from <table_schema>.<table_name>"

# connection_options = {
#     "url": "jdbc:redshift://<host>:5439/<database>",
#     "query": query,
#     "user": "<username>",
#     "password": "<password>",
#     "redshiftTmpDir": args["TempDir"]
# }

df = glueContext.create_dynamic_frame_from_options("redshift", connection_options).toDF()

# TRANSFORM: some transformation
df = df.distinct()

# LOAD: write data to Redshift
df.write.format("jdbc"). \
    option("url", "jdbc:redshift://<host>:5439/<database>"). \
    option("dbtable", "<table_schema>.<table_name>"). \
    option("user", "<username>"). \
    option("password", "<password>"). \
    mode('overwrite').save()

print("Data Loaded to Redshift")