from pyspark import SparkContext, SparkConf
import os
import sys

def define_key(number): # TODO : iteration (split /10 every time)
    return number//10

def barbaric_mean():
    dataset = "./data-1-sample.txt"
    import config
    conf = (SparkConf()
            .setAppName(config.username)           ##change app name to your username
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
    # data = sorted(data)
    # data = 
    import numpy as np
    print("ACTUAL MEAN", np.median(data))
    if count%2==0:              # If number even, need to average the two middle numbers
        m1 = data[len(data)//2-1]
        m2 = data[len(data)//2]
        m = mean(m1, m2)
    else:
        mean = mean(data[len(data)//2])

    print("COMPUTED MEAN", mean)

# def offsetmean(data, offset):
#     return data.sortByKey().

def mean(m1, m2=None):
    """ Mean function. If the number of data is even, the 2 middle elements must be averaged """
    if m2 is None:
        return m1
    else:
        return (m1+m2)*0.5
def main():
    # Expected answer on data sample : 50.642053915000005
    dataset = "./data-1-sample.txt"
    import config
    conf = (SparkConf()
            .setAppName(config.username)           ##change app name to your username
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
    counts_by_tens = data_by_keys.mapValues(len)

    # Detection of in which bag of values is the median
    acc = 0
    # cbt_tmp = sorted(counts_by_tens.collect())
    cbt_tmp = counts_by_tens.sortByKey().collect()
    for k,v in cbt_tmp:
        print(k, acc+v)
        if (acc+v>=median_pos):
            bag_i = k
            break
        acc += v
            
    print("Median is in the bag %d, with offset %d" % (bag_i, acc))
    # Print the content of the data
    d = data_by_keys.mapValues(list).lookup(bag_i)
    print(type(d))
    print(d)
    print("Count = %.8f" % count)

if __name__ == '__main__':
    # barbaric_mean()
    main()
