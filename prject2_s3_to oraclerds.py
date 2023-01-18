import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from datetime import datetime
import pg8000
import util

sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# declaring constants
job_name = 'ti_tabletimport_load'
tbl_stage = 'tablet_detail_stage'
MASTER_TABLE = 'tablet_detail'
PG_DRIVER = 'org.postgresql.Driver'


def load_staging(input_file):
    fileDF = < read file here >
    addColDF3 = fileDF.withColumn('create_time', lit(datetime.now()))
    addColDF3.createOrReplaceTempView("classification")

    validationDF = spark.sql("SELECT *, CASE \
                                            WHEN sku is null or sku_desc is null or category is null or size_code is null or \
                                                color_code is null or supplier_desc is null THEN \
                                               'mandatory fileds null' \
                                           WHEN  length(sku_desc) > 120 or length(category) > 120 or length(size_code) > 120 or length(supplier_desc) > 120 or \
                                                 length(action) > 10 THEN  \
                                               'length check failed' \
                                           WHEN lower(action) not in ('add', 'update', 'delete') THEN\
                                                'unidentified action' \
                                           WHEN COUNT(1) OVER(partition by sku) > 1 THEN \
                                                'duplicate records' \
                                           ELSE NULL \
                                      END AS error_desc \
                              FROM  classification")
    validationDF.persist()
    validationPassDF = validationDF.filter("error_desc IS NULL")
    validationFailDF = validationDF.filter("error_desc IS NOT NULL")

    validationFailDF.write \
        .format("jdbc") \
        .option("driver", PG_DRIVER) \
        .option("url", "<url_here>") \
        .option("user", "<user_here>") \
        .option("password", util.get_db_token()) \
        .option("sslmode", "require") \
        .option("dbtable", "error.tablet_detail") \
        .mode("append") \
        .save()

    validationPassDF.write \
        .format("jdbc") \
        .option("driver", PG_DRIVER) \
        .option("url", "<url_here>") \
        .option("user", "<user_here>") \
        .option("password", util.get_db_token()) \
        .option("sslmode", "require") \
        .option("dbtable", "stage.tablet_detail_stage") \
        .mode("append") \
        .save()

    validationDF.unpersist()


def load_master():
    try:
        cursor = driverConn.cursor()
        cursor.execute("""WITH merged_recs as (INSERT INTO master.tablet_detail(action, sku, sku_desc, category_detail, size_code, category_type_desc, color_detail, supplier_desc, entry_time) 
        select   CASE  WHEN lower(cgp.action) = 'add' THEN 'A' WHEN lower(cgp.action) = 'update' THEN 'U' ELSE 'D'  END as action, trim(sku), trim(sku_desc), concat(trim(category),'_',trim(category_desc)), trim(size_code), trim(category_type_desc), concat(trim(color_code),'_',trim(color_desc)), trim(supplier_desc), CURRENT_TIMESTAMP
                                               from stage.tablet_detail_stage cgp 
                          ON CONFLICT  on sku
                          DO UPDATE SET   sku_desc = excluded.sku_desc,
                                    size_code = excluded.size_code,
                                    category_type_desc = excluded.category_type_desc,
                                    supplier_desc = excluded.supplier_desc,
                                    entry_time = CURRENT_TIMESTAMP
                          RETURNING  sku) """)

        driverConn.commit()
    except Exception as e:

        driverConn.rollback()
        raise
    finally:
        cursor.close()

### EXECUTION STARTING FROM THIS TRY BLOCK
try:

    job_start_time = datetime.now()

    input_file = ''
    job_end_time = datetime.now()
    args = getResolvedOptions(sys.argv, ['input_file_name'])
    input_file = args['input_file_name']
    logger.info("input_file : " + input_file)
    logger.info("Job Name" + job_name)
    driverConn = util.get_connection()

    if input_file.strip().upper() == 'NONE':

        logger.info("Job completed successfully")
    else:

        util.truncate_staging_table(driverConn, tbl_stage)

        load_staging(input_file)

        load_master()



except Exception as e:
    error_msg = str(e)
    raise
finally:
    driverConn.close()