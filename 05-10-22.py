from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdatafile\\us-500.csv"
df=spark.read.format("csv").option("header",True).option("inferSchema",True).load(data)
df.show()
#Mask The Number
from pyspark.sql import *
num=102938776
strg=(str(num))
n=len(strg)
card=list(strg)
card[2:int(n)-2]='x'*int(n-4)
mynew=''.join(card)
print(mynew)

#Mask the email id
def mask_email_func(colval):
    mail_usr=colval.split("@")[0]
    n=len(mail_usr)
    charlist=list(mail_usr)
    charlist[1:int(n)-1]='x'*int(n-2)
    out=''.join(charlist)+'@'+colval.split("@")[1]
    return out
res=mask_email_func("nikhilpatil@gamil.com")
print(res)

# mask the charcter
strg="iamfromjalgaon"
n=len(strg)
print(n)
print(len("iaxxxxxxxxxxon"))
card=list(strg)
card[2:int(n)-2]='x'*int(n-4)
mynew=''.join(card)
print(mynew)