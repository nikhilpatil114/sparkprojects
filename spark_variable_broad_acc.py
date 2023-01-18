from pyspark import SparkContext
sc = SparkContext("local", "Broadcast app")
words_new = sc.broadcast(["shreyash", "rishabh", "Shubham", "Sagar", "Mehul"])
data = words_new.value
print ("Stored data -> %s" % (data))
elem = words_new.value[2]
print ("Printing a particular element in RDD -> %s" % (elem))



num = sc.accumulator(1)
def f(x):
  global num
  num+=x
rdd = sc.parallelize([2,3,4,5])
rdd.foreach(f)
final = num.value
print ("Accumulated value is -> %i" % (final))
