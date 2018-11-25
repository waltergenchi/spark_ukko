from pyspark import SparkContext, SparkConf
from operator import add
import os
import sys

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
#dataset = "/proj/group/distributed-data-infra/data-1-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-1.txt"
dataset = "data-2-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-2.txt"

conf = (SparkConf()
        .setAppName("genchi")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "20")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)

data = sc.textFile(dataset)

# matrix A is data_left, each element is of form: (i, j, value) => (j, (i, value))
data_left = data.flatMap(lambda line: [float(x) for x in line.split()]) \
	.zipWithIndex().map(lambda (value, index): (index - (index // 1000) * 1000, (index // 1000, value))) 

# the data left matrix is joined with itself in order to do the multiplication A * A_transpose
# each element of the matrix A has the following form: (j, k, value) => (j, (k, value));
# for A_transpose the lines becomes columns so we need to map it as: (j, k, value) transpose => (k, j, value) => (j, (k, value)) 
data_left  = data_left.join(data_left).map(lambda (j, ((i, value1), (k, value2))): ((i, k), value1 * value2)).reduceByKey(add).map(lambda ((i, k), v): (k, (i, v)))

# data_right is the second A matrix from A * A_transpose * A
data_right = data.flatMap(lambda line: [float(x) for x in line.split()]) \
	.zipWithIndex().map(lambda (value, index): (index // 1000, (index - (index // 1000) * 1000, value)))

data_product = data_left.join(data_right).map(lambda (j, ((i, value1), (k, value2))): ((i, k), value1 * value2)).reduceByKey(add).sortByKey()
 
st = ""
with open("cacca.txt", "w") as f:
	for (index, value) in data_product.toLocalIterator():
		st = st + str(value) + " "
		if (index[1] % 999 == 0) and (index[1] != 0):
			st = st + "\n"
			f.write(st)
			st = ""
