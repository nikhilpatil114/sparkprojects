from pyspark.sql.types import *
Schema=StructType([
  StructField(Store,StringType(),nullable=True),
  StructField(StoreType,StringType(),nullable=True),
  StructField(Assortment,StringType(),nullable=True),
  StructField(CompetitionDistance,FloatType(),nullable=True),
  StructField(CompetitionOpenSinceMonth,IntegerType(),nullable=True),
  StructField(CompetitionOpenSinceYear,IntegerType(),nullable=True),
  StructField(Promo’,IntegerType(),nullable=True),
  StructField(Promo2SinceWeek,IntegerType(),nullable=True),
  StructField(Promo2SinceYear,IntegerType(),nullable=True),
  StructField(PromoInterval,StringType(),nullable=True)
])
df = spark.read.option(header,True).schema(Schema).csv(store.csv)
df.show()