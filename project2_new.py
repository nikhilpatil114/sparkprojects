import pyspark
import cx_Oracle
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdatafile\\proj2.csv"
df=spark.read.format("csv").option("header",True).option("inferSchema",True).load(data)
#df.show()
con = cx_Oracle.connect('hr/hr@orclpdb')
cur=con.cursor()
qry="insert into emp values(30,'mahesh',5000)"
qry1="delete from emp where eid=10"
cur.execute(qry)
cur.execute(qry1)
con.commit()
'''res = cur.fetchall()
for row in res:
    print(row)
'''