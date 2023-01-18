from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="E:\\bigdatafile\\tab.csv"


def Move():
    for db in df_json.columns:
        # Creating table list from the dataframe

        tbl_list = df_json.select(db).rdd.map(lambda x: x[0]).collect()
        print(db)