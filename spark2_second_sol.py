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
    dataset = "stupido.txt" # dataset with 10^3 rows, work fine in almost 5 seconds

    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "10")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    data_file = sc.textFile(dataset)

    print("\n\nReading file and converting numbers in float for each line")
    matrix = data_file.map(lambda line: line.split()).map(lambda value: [float(i) for i in value]).zipWithIndex().map(lambda (vals,index): (index//10000,vals))
    
    
    print("\n\n**** Matrix A ****")
    print("Computing the number of rows of A\n")
    nRows=matrix.count()
    print("NUMBER OF ROWS of A: %d\n" %nRows)

    print("Computing the number of columns of A\n")
    Col = matrix.take(1)
    nCols = [len(x)for x in Col]
    print("NUMBER OF COLUMNS of A: %d\n" % nCols[0] )



    print("\n\n**** Matrix A_transpose * A ****")

    print("\n ** Mapping Operation ** \n")
    start_map1 = time.time()
    Atranspose_A = matrix.map(lambda row: multiply(row[1]))#.zipWithIndex()
    #Atranspose_A = Atranspose_A.map(lambda (vals,index): (index//10000,vals))
    end_map1 = time.time()
    takenTime_map1 = end_map1-start_map1
    print("    TAKEN TIME by MAPPING TRANSFORMATION: %f" %takenTime_map1)

    print("\n ** Reduce Operation ** \n")
    start_reduce1 = time.time()
    Atranspose_A = Atranspose_A.reduceByKey(add)
    print("*****\n\n\n")
    print(A_Atranspose_A.collect())
    Atranspose_A = Atranspose_A.reduce(add)
    end_reduce1 = time.time()
    takenTime_reduce1 = end_reduce1-start_reduce1
    print("    TAKEN TIME by REDUCE ACTION: %f" %takenTime_reduce1)


    print("\n\nThe SHAPE of A_transpose * A is (%d,%d)" %(Atranspose_A.shape[0],Atranspose_A.shape[1]))
    print("The TYPE of A_transpose * A is %s" %type(Atranspose_A))



    print("\n\n\n**** Matrix A * A_transpose * A ****")

    print("\n ** Mapping Operation ** \n")
    start_map2 = time.time()
    A_Atranspose_A=matrix.map(lambda line: (line[0],list(np.dot(line[1],Atranspose_A)))).sortByKey() # very small amount of time
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
    print(A_Atranspose_A.collect())

    # saving file as:
    # A_Atranspose_A=A_Atranspose_A.map(lambda line: str(row[1]))
    # A_Atranspose_A.saveAsTextFile("output2_genchi.txt")

if __name__ == "__main__":
    main()