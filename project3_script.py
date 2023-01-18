1.
Copy
from s3 raw

to
processed.


def Move():
    for db in df_json.columns:
        # Creating table list from the dataframe

        tbl_list = df_json.select(db).rdd.map(lambda x: x[0]).collect()
        print(db)

        for tbl in tbl_list[0]:
            try:
                # Creating variable which stores the path for storeing final dataframe in s3 processed.

                processedpath = 's3://path/' + str(db) + '/' + str(tbl) + '/' + str(st) + '/' + str(h) + '/'
                print("Start reading the table names")

                # creating dynamic dataframe using glucontext
                pushDF = glueContext.create_dynamic_frame.from_options(connection_type="parquet", connection_options={
                    'paths': ["s3://path/Lambda_data_dump/" + db + "/" + tbl + "/"]}).toDF()

                print("load the table in pushDF", tbl)

                # Writing dataframe to processed path
                pushDF.coalesce(1).write.format('parquet').save(processedpath)  # Writing files into processed path
                print("Writing the table ", tbl)
            except:
                print('TABLE DOES NOT EXIST --' + tbl)


Move()

2.
Masking & cleaning
sample

Masking()


# Creating Function for sysobject table and applying withcolumn when function of pyspark.

def Obh():
    tbl = 'Objects'
    for db in lst:
        # storing parquet file path to variable and using it for creating & writing dataframe
        object_path = 's3://path/' + str(db) + '/' + str(tbl) + '/' + str(st) + '/' + str(h) + '/'
        processedpath = 's3://path/' + str(db) + '/' + str(tbl) + '/' + str(st) + '/' + str(h) + '/'

        print(db, object_path)

        # creating dataframe.
        df = spark.read.parquet(object_path)

        df_final = df.withColumn('new', when(df.Object == 'ram', df.TextVal1 == '200sad')).drop('new')
        print(df_final)

        # Writing dataframe to processed path by using selected columns.
        df_final.coalesce(1).write.parquet(processedpath)


obh()

-- Total
numbers
of
bucket in our
projects
are
5

3.
from process to

redshift


def load():
    try:
        print("Started executiing load_objects")
        for i in listofdatabases:
            # reading parquet file from s3 for each table present in s3
            pushDF = glueContext.create_dynamic_frame.from_options(connection_type="parquet", connection_options={
                'paths': ["s3://path/" + i + "/Objects/"]}).toDF()
            # load associated customerid for into variable from cutomerbase table
            var1 = customerbaseDF.filter(customerbaseDF.customername == i).select('customerid').rdd.first()[0]
            # Casting byte datatyes of columns into integer one
            pushDF = pushDF.withColumn("Object", pushDF.IObject.cast(IntegerType()))\
                .withColumn("IsDeleted", pushDF.IsDeleted.cast( IntegerType()))\
                .withColumn("BlobVal1", pushDF.BlobVal1.cast(StringType()))\
                .withColumn("IsTemplate",pushDF.IsTemplate.cast(IntegerType()))
            # Because of constraints in redshift that column name should not be ID we change the column name to ids
            pushDF = pushDF.withColumnRenamed("ID", "ids")
            # assign a customerid from customer base table
            pushDF = pushDF.withColumn("customerid", lit(var1))
            # Selecting those columns which we required in redshift tables
            pushDF = pushDF.select("ids", "Object", "ObjectDes", "OType", "IsSObject", "Created", "Modified",
                                   "CreatedBy", "ModifiedBy", "ParentID", "IntVal1")
            rsetsdf = glueContext.create_dynamic_frame_from_options("redshift", connection_smaster).toDF()
            # join the the table with rsetmaster to get assetmasterid from that table
            resultdf = pushDF.join(rsetsdf, (pushDF.ids == rsetsdf.ids) & (pushDF.customerid == rsetsdf.customerid),
                                   "left").select(pushDF["*"], rsetsdf["masterid"])
            dynamic_resultdf = DynamicFrame.fromDF(resultdf, glueContext, "nested")
            # writing the final dataframe into Redshift
            glueContext.write_dynamic_frame.from_jdbc_conf(dynamic_resultdf, catalog_connection="Redshift",
                                                           connection_options={"dbtable": "bi.sysobjsmaster",
                                                                               "database": "sandbox"},
                                                           redshift_tmp_dir=args["TempDir"])
        resultdf.show()
        resultdf.printSchema()
    except:
        print("Table is not present in " + i)
