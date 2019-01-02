##### WALTER GENCHI - 014961054 ######

from pyspark import SparkContext, SparkConf
import os
import sys

import numpy as np

def naive_median():
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

    print("NUMPY MEDIAN", np.median(data))

    # If count is an even number, we need to average the two middle numbers
    if count%2==0:
        m1 = data[len(data)//2-1]
        print(m1)
        m2 = data[len(data)//2]
        print(m2)
        mean_comp = (m1+m2)/2
    else:
        mean_comp= data[len(data)//2]

    print("COMPUTED MEDIAN WITH SORTING OPERATION", mean_comp)

def main():
    # Expected median on data sample : 50.642053915000005 (computed with naive_median function)
    dataset = "data-1.txt"
    n_of_bags = 10

    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "5")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    print("Reading values and converting them to floats\n")
    #and assigning them to the bag, e.g. 45 -> (4,45)")
    data = sc.textFile(dataset)\
             .map(lambda n: float(n)) # every number is mapped to his bag, e.g. 45 -> (4,45)

    print("Making data persistent across future operations\n")
    data.persist()

    print ("Counting data\n")
    count = data.count() #useful for computing the position of the median later
    print("Count = %d\n" %count)

    print("Computing Minimum\n")
    min_v = data.min() # useful for normalization
    print("Minimum = %d\n" %min_v)

    print("Computing Maximum\n")
    max_v = data.max() # useful for normalization
    print("Maximum = %d\n" %max_v)

    print("Every number n is mapped to his bag using:\n 1) Normalization, i.e. x=(n-min)/(max-min)\n 2) Assigned Bag = int(x * n_of_bags)\n 3) FINAL RESULT: n -> (Assigned Bag,n)")
    data=data.map(lambda n: ( int(( ( n- min_v) / (max_v-min_v) ) * n_of_bags) , n ))

    median_pos = count//2
    print("The position of the median is %d" %median_pos)

    print("\n\n\n ***** reduceByKey ***** \n\n\n ")
    print("Creating the bags, i.e. get something like\n[(0,[numbers between 0 and 0.0999]) , ... , (10,[numbers between 0.9 and 1])]")
    bag_by_values = data.groupByKey()

    #data_by_keys = data.reduceByKey() is it more efficient?
    print("\n\n\n ***** mapValues ***** \n\n\n ")
    print("Creating the bag size (ordered by key, i.e. number of bag), get something like\n[(0,[how many numbers between 0 and 0.0999]) , ... , (10,[how many numbers between 0.9 and 1])]")
    bag_by_numerosity = bag_by_values.mapValues(len).sortByKey().values().collect()


    print("Determine in which bag is the median\n")
    tmp = 0
    bag_median = 0

    for i in range(10):
        print("Number of bag: %d, Position of the first element in the bag: %d" % (i,tmp+i))
        if (tmp + bag_by_numerosity[i] >= median_pos):
            bag_median = i #this is the bag which contains the median
            break
        tmp = tmp + bag_by_numerosity[i]
            
    print("\nMedian is in the bag %d, with offest %d\n" % (bag_median, tmp))

    print("Sorting the bag where the median is contained\n")
    d = sorted(bag_by_values.mapValues(list).lookup(bag_median)[0])

    med=d[median_pos-tmp]
    if count % 2 == 0:
        m1 = d[median_pos-tmp-1]
        m2 = d[median_pos-tmp]
        med_comp = (m1+m2)/2
    else:
        med_comp= d[median_pos-acc]

    print("The median is %.16f in position %d\n" % (med_comp, median_pos))
    # save median value (16-float) to file
    file = open("output1_genchi.txt","w")
    file.write(str(med_comp))


if __name__ == '__main__':
    #naive_median()
    main()
