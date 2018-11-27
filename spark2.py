##### WALTER GENCHI - 014961054 #####

from pyspark import SparkConf,SparkContext
import sys
import numpy as np
from operator import add
import time

# the outer product is function used for the first mapping (i.e. multiplication A_t + A)
def multiply(row):
    return np.outer(row,row)

def main():
    #dataset = "data-2-105.txt" # dataset with 10^5 rows, works fine in almost 70 seconds
    dataset = "data-2.txt" # dataset with 10^3 rows, work fine in almost 5 seconds

    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "20")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    data_file = sc.textFile(dataset)
    print(type(data_file))
    # Read matrix from file and split the lines based on space and use float for items
    print("\n\nReading file and converting numbers in float for each line")
    matrix = data_file.map(lambda line: line.split()).map(lambda value: [float(i) for i in value])
    
    '''
    print("\n\n**** Matrix A ****")
    print("Computing the number of rows of A\n")
    nRows=matrix.count()
    print("NUMBER OF ROWS of A: %d\n" %nRows)

    print("Computing the number of columns of A\n")
    Col = matrix.take(1)
    nCols = [len(x)for x in Col]
    print("NUMBER OF COLUMNS of A: %d\n" % nCols[0] )
    '''



    print("\n\n**** Matrix A_transpose * A ****")

    print("\n ** Mapping Operation ** \n")
    start_map1 = time.time()
    Atranspose_A = matrix.map(lambda row: multiply(row)).zipWithIndex()
    print("\n\n\n\n\n")
    Atranspose_A = Atranspose_A.map(lambda (vals,index): (index//1000,vals)) # very small amount of time
    print("\n\n\n\n\n")
    end_map1 = time.time()
    takenTime_map1 = end_map1-start_map1
    print("    TAKEN TIME by MAPPING TRANSFORMATION: %f" %takenTime_map1)

    print(Atranspose_A.take(1))
    #print(Atranspose_A.take(1)[0].shape)


    print("\n ** Reduce Operation ** \n")
    start_reduce1 = time.time()
    print("\n\n\n\n\n")
    Atranspose_A = Atranspose_A.reduceByKey(add) #immediate

    print("***********\n\n\n\n\n***********")

    Atranspose_A = Atranspose_A.map(lambda (index,vals): vals).reduce(add) #67 seconds

    '''
    print(Atranspose_A.take(2)[0])
    print("\n\n\n\n\n\n\n")
    print(Atranspose_A.take(2)[1])
    print("\n\n\n\n\n\n\n")
    print(Atranspose_A.take(5))
    '''

    #reduce(add) # bottlneck, ~56 seconds with 10^5 rows vs. ~3 seconds with 10^3 rows
    end_reduce1 = time.time()
    takenTime_reduce1 = end_reduce1-start_reduce1
    print("    TAKEN TIME by REDUCE ACTION: %f" %takenTime_reduce1)


    print("\n\nThe SHAPE of A_transpose * A is (%d,%d)" %(Atranspose_A.shape[0],Atranspose_A.shape[1]))
    print("The TYPE of A_transpose * A is %s" %type(Atranspose_A))



    print("\n\n\n**** Matrix A * A_transpose * A ****")

    print("\n ** Mapping Operation ** \n")
    start_map2 = time.time()
    A_Atranspose_A=matrix.map(lambda line: list(np.dot(line,Atranspose_A))) # very small amount of time
    end_map2 = time.time()
    takenTime_map2 = end_map2-start_map2
    print("    TAKEN TIME by MAPPING TRANSFORMATION: %f" %takenTime_map2)


    print("\n ** Count Operation ** \n")
    start_count = time.time()
    nRows=A_Atranspose_A.count()
    end_count = time.time()
    takenTime_count = end_count-start_count
    print("    TAKEN TIME by COUNT ACTION: %f" %takenTime_count) # ~12 seconds with 10^5 rows vs. ~2 seconds with 10^3 rows
    

    print("\n\nFINAL NUMBER OF ROWS: %d" %nRows)
    Col = A_Atranspose_A.take(1)
    nCol = [len(x)for x in Col]
    print("FINAL NUMBER OF COLUMNS : %d" %nCol[0])

    print("The type of A * A_transpose * A is %s\n\n" %type(Atranspose_A))

if __name__ == "__main__":
    main()