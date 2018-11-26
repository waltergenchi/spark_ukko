from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg.distributed import *
from pyspark.sql import SparkSession
import os
import operator
import sys
import time

starttime = time.time() #getting time for measurements

dataset = "data-2.txt" ##change this to your dataset path
output = "pippo_provissima.txt"

conf = (SparkConf()
        .setAppName("genchi")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)


data = sc.textFile(dataset)
data = data.map(lambda s: s.split()).map(lambda row:[float(s) for s in row]).zipWithIndex().map(lambda xi: IndexedRow(xi[1], xi[0]))

spark = SparkSession(sc) #to work with matrices Spark uses some kind of SQL stuff which is started here.

mat = IndexedRowMatrix(data).toBlockMatrix()
matrixxx = mat.multiply(mat.transpose()).multiply(mat)

localMatrix = matrixxx.toLocalMatrix()
print(localMatrix.numCols) #checking for correct size
print(localMatrix.numRows)

endtime = time.time()
takenTime = endtime-starttime

print"TIME TAKEN",takenTime


print "FINISHED..........................................................."