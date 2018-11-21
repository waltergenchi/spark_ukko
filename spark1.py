from pyspark import SparkContext, SparkConf
import os
import sys

import numpy as np

# given an input for the number 
def define_key(number):
    return number//100

def mean(m1, m2=None):
#Mean function. If the number of data is even, the 2 middle elements must be averaged
    if m2 is None:
        return m1
    else:
        return (m1+m2)*0.5

def barbaric_median():
    dataset = "data-1.txt"
    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "40")  ##dont be too greedy ;)
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
            .set("spark.cores.max", "30")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    print("Reading values, converting them to floats and assigning them to the bag, e.g. 45 -> (4,45)")
    data = sc.textFile(dataset)\
             .map(lambda n: (float(n)//10, float(n))) # every number is mapped to his bag, e.g. 45 -> (4,45)

    print("Making data persistent across future operations")
    data.persist()
    print ("Counting data")
    count = data.count()

    # If the whole dataset is sorted -- it won't ! --, the position of
    # the median should be in the middle
    median_pos = count//2
    print("The position of the median is %d" %median_pos)
    
    #groupByKey vs reduceByKey !!
    print("Creating the bags, i.e. get something like [(0,[numbers between 0 to 9.999]) , ... , (10,[numbers between 90 and 100])]")
    bag_by_values = data.groupByKey()

    print("*****")
    #print(data_by_keys.keep(10).mapValues(list))
    print("*****")

    #data_by_keys = data.reduceByKey() is it more efficient?
    print("Creating the quantity of elements in the bag (ordered by key, i.e. number of bag), get something like [(0,[how many numbers between 0 and 9.999]) , ... , (10,[how many numbers between 90 and 100])]")
    bag_by_numerosity = data_by_keys.mapValues(len).sortByKey().collect()

    print("*****")
    #counts_by_tens.take(10).foreach(println)
    print("*****")

    #counts_by_tens = data.aggregateByKey() should be the most efficient!

    print("Detect in which bag of values is the median")

    tmp = 0
    bag_median = 0

    print("Determine in which bag is the median")
    # cbt_tmp is an iterator, k si the key, v is the value
    for i in range(10):
        print("Number of bag: %d, Position of the first element in the bag: %d" % (i,tmp+i))
        if (tmp + bag_by_numerosity[i] >= median_pos):
            bag_median = i #this is the bag which contains the median
            break
        tmp = tmp + bag_by_numerosity[i]
            
    print("Median is in the bag %d, with offest %d" % (bag_i, tmp))

    # Print the content of the data
    d = sorted(bag_by_values.mapValues(list).lookup(bag_i)[0])

    med=d[median_pos-tmp]
    if count % 2 == 0:
        m1 = d[median_pos-tmp-1]
        m2 = d[median_pos-tmp]
        med_comp = mean(m1, m2)
    else:
        med_comp= mean(d[median_pos-acc])

    print("The median is %.16f in position %d" % med_comp, median_pos)

    print("The count is %.8f" % count)

if __name__ == '__main__':
    #barbaric_median()
    main()
