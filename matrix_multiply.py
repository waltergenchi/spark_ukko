from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import sys
import numpy as np
from operator import add
#from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, BlockMatrix
import time

def multiply(row):
    return np.outer(row,row)
    '''
    multiply_row = []
    for i in row:
        for j in row:
            multiply = float(i) *float(j)
            multiply_row.append(multiply)
            print(multiply_row)
    return multiply_row
    '''

def sum_values(mat):
    return np.add(mat)
    #return tuple(sum(x) for x in zip(a,b))

def main():
    dataset = "data-2-105.txt"
    #dataset = "data-2-sample.txt"

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
    # Read matrix from file and split the lines based on space and use float for items
    print("\n\nReading file and converting numbers in float for each line")
    matrix = data_file.map(lambda line: line.split()).map(lambda value: [float(i) for i in value])
    
    #print(matrix.collect())

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
    Atranspose_A = matrix.map(lambda row: multiply(row))
    end_map1 = time.time()
    takenTime_map1 = end_map1-start_map1
    print("    TAKEN TIME by MAPPING TRANSFORMATION: %f" %takenTime_map1)
    #print(row_permutation.collect())
    print("\n ** Reduce Operation ** \n")
    start_reduce1 = time.time()
    Atranspose_A = Atranspose_A.reduce(add)
    end_reduce1 = time.time()
    takenTime_reduce1 = end_reduce1-start_reduce1
    print("    TAKEN TIME by REDUCE ACTION: %f" %takenTime_reduce1)

    print("\n\nThe SHAPE of A_transpose * A is (%d,%d)" %(Atranspose_A.shape[0],Atranspose_A.shape[1]))
    print("The TYPE of A_transpose * A is %s" %type(Atranspose_A))



    print("\n\n\n**** Matrix A * A_transpose * A ****")

    print("\n ** Mapping Operation ** \n")
    start_map2 = time.time()
    A_Atranspose_A=matrix.map(lambda line: list(np.dot(line,Atranspose_A)))
    end_map2 = time.time()
    takenTime_map2 = end_map2-start_map2
    print("    TAKEN TIME by MAPPING TRANSFORMATION: %f" %takenTime_map2)

    start_count = time.time()
    nRows=A_Atranspose_A.count()
    end_count = time.time()
    print("\n ** Count Operation ** \n")
    takenTime_count = end_count-start_count
    print("    TAKEN TIME by COUNT ACTION: %f" %takenTime_count)
    print("\n\nFINAL NUMBER OF ROWS: %d" %nRows)

    Col = A_Atranspose_A.take(1)
    nCol = [len(x)for x in Col]
    print("FINAL NUMBER OF COLUMNS : %d" %nCol[0])

    print("The type of A * A_transpose * A is %s\n\n" %type(Atranspose_A))

    #print(len(ris.collect()))
    #print(ris.collect())
    '''
    def toCSVLine(data):
        return ','.join(str(d) for d in data)

    lines = ris.map(toCSVLine)
    lines.saveAsTextFile('pippo.csv')
    '''
    '''
    for i in range(row_permutation.shape[1]):
        ris=matrix.map(lambda line: (i, np.dot(line,row_permutation[:,i])))
        print(ris.collect())
    print(ris.collect) 
    '''
    '''
    # need a SQLContext() to generate an IndexedRowMatrix from RDD
    rdd=sc.parallelize(row_permutation)
    print(type(rdd))
    print(rdd.collect())
    sqlContext = SQLContext(sc)
    rows = IndexedRowMatrix(rdd).toBlockMatrix()
    Xirm.rows.map(lambda x: (lu[x.index], *x.vector.toArray().tolist()))
    
    rows_2 = IndexedRowMatrix( \
        matrix \
        .map(lambda row: IndexedRow(row[1], row[0])) \
        ).toBlockMatrix()

    mat_product = rows.multiply(rows_2)
    '''
    '''
    print(row_permutation[0])
    a=row_permutation[0]
    np.reshape(a,(2,2))
    print(a.shape)
    '''
    #matrix_chunks = chunks(row_permutation,nCol[0])

# open a file to write the matrix output on local and write in required format
    '''
    filelocation = 'pippo_2.txt'

    t_file = open(filelocation,'w')
    i =1
    for num in ris.collect():
        if(i % nCol[0] == 0):
            t_file.write("%s" % num + "\n")

        else:
            t_file.write("%s" % num + " ")
        i = i+1
    '''

# below lines worte the output in spark format using save as a text file : uncomment if running on cluster
   # matrix_unformated = sc.parallelize(matrix_chunks).coalesce(1)

    #matrix_formated = (matrix_unformated.map(lambda m: ' '.join(map(str, m))))

    #matrix_formated.saveAsTextFile(sys.argv[2])
    

if __name__ == "__main__":
    main()