from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import sys
import numpy as np
from operator import add
#from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, BlockMatrix

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
    dataset = "data-2-sample.txt"

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
    print("Reading file and converting numbers in float for each line\n")
    matrix = data_file.map(lambda line: line.split()).map(lambda value: [float(i) for i in value])
    
    #print(matrix.collect())

    print("Computing the number of rows of A\n")
    nRows=matrix.count()
    print("Number of rows of A: %d\n",nRows)


    print("Computing the number of columns of A\n")
    Col = matrix.take(1)
    nCols = [len(x)for x in Col]
    print("Number of columns of A: %d\n",nCol[0])

    print("\n\n\n ***** Mapping Operation ***** \n\n\n")
    Atranspose_A = matrix.map(lambda row: multiply(row))
    #print(row_permutation.collect())
    print("\n\n\n ***** Reduce Operation ***** \n\n\n")
    Atranspose_A = Atranspose_A.reduce(add)

    print("The shape of A_transpose * A is %f\n",Atranspose_A.shape)
    print("The type of A_transpose * A is %s\n",type(Atranspose_A))

    A_Atranspose_A=matrix.map(lambda line: list(np.dot(line,row_permutation)))

    print("Computing the number of rows of A * A_transpose * A\n")
    nRows=A_Atranspose_A.count()
    print("Number of rows: %d\n",nRows)


    print("Computing the number of columns of A * A_transpose * A\n")
    Col = A_Atranspose_A.take(1)
    nCols = [len(x)for x in Col]
    print("Number of columns of A * A_transpose * A: %d\n",nCol[0])

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