from pyspark import SparkContext, SparkConf
#from pyspark.sql import SparkSession
from operator import add
import os
import sys

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
#dataset = "/proj/group/distributed-data-infra/data-1-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-1.txt"
dataset = "data-2.txt"
#dataset = "/proj/group/distributed-data-infra/data-2.txt"

conf = (SparkConf()
        .setAppName("genchi")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "30")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)
#spark = SparkSession(sc)

data = sc.textFile(dataset)

# (i, j, value) => (j, (i, value))
#data_left = data.flatMap(lambda line: [float(x) for x in line.split()]) \
#	.zipWithIndex().map(lambda (value, index): (index - (index // 1000) * 1000, (index // 1000, value)))

# (j, k, value) => (j, (k, value)) for a matrix A; for A transpose the lines becomes columns so we need to map it as: (j, k, , value) transpose => (k, j, value) => (j, (k, value))
#data_center = data.flatMap(lambda line: [float(x) for x in line.split()]) \
#	.zipWithIndex().map(lambda (value, index): (index  - (index // 1000) * 1000, (index // 1000, value)))

#data_first_product = data_left.join(data_center).map(lambda (j, ((i, value1), (k, value2))): ((i, k), value1 * value2)).reduceByKey(add).map(lambda ((i, k), v): (k, (i, v)))

#data_right = data.flatMap(lambda line: [float(x) for x in line.split()]) \
#	.zipWithIndex().map(lambda (value, index): (index // 1000, (index - (index // 1000) * 1000, value)))

#data_product = data_first_product.join(data_right).map(lambda (j, ((i, value1), (k, value2))): ((i, k), value1 * value2)).reduceByKey(add).sortByKey()
print("pippo")
data_left = data.flatMap(lambda line: [float(x) for x in line.split()]) \
        .zipWithIndex().map(lambda (value, index): (index // 1000, [value])).reduceByKey(add)
print("marketa")

data_center = data_left
#
data_right = data.flatMap(lambda line: [float(x) for x in line.split()]) \
        .zipWithIndex().map(lambda (value, index): (index - (index // 1000) * 1000, [value])).reduceByKey(add)
print("neandertal")

data_first_product = data_left.join(data_center).map(lambda (index, (value1, value2) ) : (index, ([a * b for a, b in zip(value1, value2)]))).reduceByKey(add)
print("blanca")
data_product = data_first_product.join(data_right).map(lambda (index, (value1, value2) ) : (index, ([a * b for a,b in zip(value1, value2)]))).reduceByKey(add).sortByKey()
print("katerina")

ii = 0
for i in data_product.collect():
        print i
        if ii > 20:
                break
        ii = ii + 1

#ii = 0
#st = ""
#with open("/wrk/comanesc/comanesc_matrix_output.txt", "w") as f:
#	for (index, value) in data_product.toLocalIterator():
#               ii = ii + 1
#               st = st + str(value) + " "
#               if (index[1] % 999 == 0) and (index[1] != 0):
#                       st = st + "\n"
#                       f.write(st)
#                       st = ""
#