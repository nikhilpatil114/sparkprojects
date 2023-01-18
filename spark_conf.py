spark.conf.set("spark.executor.instances", 4)
spark.conf.set("spark.executor.cores", 4)

#####################     Dynamic alocation of executor and core

spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.executor.cores", 4)
spark.conf.set("spark.dynamicAllocation.minExecutors","1")
spark.conf.set("spark.dynamicAllocation.maxExecutors","5")

#####################
saprk submit  --maste yarn --deploy mode cluster --num-executors 6 --executor-cores 5 --executor-memory 19