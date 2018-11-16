from pyspark import SparkContext, SparkConf
import os
import sys

def define_key(number): # TODO : iteration (split /10 every time)
    return number//10

def barbaric_median():
    dataset = "data-1-sample.txt"
    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "10")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    data = sc.textFile(dataset)\
             .map(lambda s: float(s))
    count = data.count()
    data=data.collect()
    data = sorted(data)
    # data = 
    import numpy as np

    def mean(m1, m2=None):
    #Mean function. If the number of data is even, the 2 middle elements must be averaged
        if m2 is None:
            return m1
        else:
            return (m1+m2)*0.5

    print("ACTUAL MEDIAN", np.median(data))
    # If number even, need to average the two middle numbers
    if count%2==0:
        m1 = data[len(data)//2-1]
        print(m1)
        m2 = data[len(data)//2]
        print(m2)
        mean_comp = mean(m1, m2)
    else:
        mean_comp= mean(data[len(data)//2])

    print("COMPUTED MEDIAN", mean_comp)

# def offsetmean(data, offset):
#     return data.sortByKey().

def main():
    # Expected answer on data sample : 50.642053915000005
    dataset = "data-1-sample.txt"
    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "10")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    data = sc.textFile(dataset)\
             .map(lambda s: float(s))\
             .map(lambda n: (define_key(n), n))
    count = data.count()

    # If the whole dataset is sorted -- it won't ! --, the position of
    # the median should be in the middle
    median_pos = count//2
    #groupByKey vs reduceByKey !!
    data_by_keys = data.groupByKey()

    print("*****")
    print(data_by_keys.keep(10).mapValues(list))
    print("*****")

    #data_by_keys = data.reduceByKey() is it more efficient?
    counts_by_tens = data_by_keys.mapValues(len)

    print("*****")
    #counts_by_tens.take(10).foreach(println)
    print("*****")

    # Detection of in which bag of values is the median
    acc = 0
    # cbt_tmp = sorted(counts_by_tens.collect())
    # is it less efficient?
    cbt_tmp = counts_by_tens.sortByKey().collect()
    # cbt_tmp e un iteratore, k e key, v e value
    for k,v in cbt_tmp:
        print(k, acc+v) # print (number of bag, position of the first element of the bag) 
        if (acc+v >= median_pos):
            bag_i = k #this is the bag which contains the median
            break
        acc += v
            
    print("Median is in the bag %d, with offset %d" % (bag_i, acc))
    # Print the content of the data
    d = data_by_keys.mapValues(list).lookup(bag_i)
    print(type(d))
    print(d)
    print("Count = %.8f" % count)

if __name__ == '__main__':
    #barbaric_median()
    main()
