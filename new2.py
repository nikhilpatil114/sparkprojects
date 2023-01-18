import pandas as pd

data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54], ['Ann', 34]]
data1=["jay","ram","sham","mohan"]
# Create the pandas DataFrame
pandasDF = pd.DataFrame(data, columns=['Name', 'Age'])
pandasDF1 = pd.DataFrame(data1, columns=['Name'])

# print dataframe.
print(pandasDF)

print(pandasDF1)

from pyspark.sql import SparkSession
#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
#Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(pandasDF)
#sparkDF.show()
arr=[2,3,4,5,6,7]
arr2=arr[::-1]
print(arr2)

