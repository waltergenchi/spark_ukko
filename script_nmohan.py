from pyspark import SparkContext, SparkConf
import os
import sys

dataset = "data-1-sample.txt" ##change this to your dataset path

conf = (SparkConf()
        .setAppName("genchi")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)

data = sc.textFile(dataset)
data = data.map(lambda s: float(s))

#count = data.count()
sum = data.sum()

print "Count = %.8f" % count
print "Sum = %.8f" % sum
