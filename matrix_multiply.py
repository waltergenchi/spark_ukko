__author__ = 'pranavgoel'

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import sys
import numpy as np
from operator import add
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, BlockMatrix

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

def chunks(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]

def sum_values(mat):
    return np.add(mat)
    #return tuple(sum(x) for x in zip(a,b))

def main():
    dataset = "data-2-sample.txt"

    conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "60")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )
    sc = SparkContext(conf=conf)

    raw_matrix_file = sc.textFile(dataset)
    # Read matrix from file and split the lines based on space and use float for items
    matrix = raw_matrix_file.map(lambda line: line.split()).map(lambda value: [float(i) for i in value])
    #print(matrix.collect())
    print("read file")

    
    #print(len(matrix.count))
    #Col = matrix.take(1)

    #nCol = [len(x)for x in Col]
# doing purmutation on the row by row for example a b = aa ab ba bb

# doing the sum coloumn by coloumn
    print("**************\n\n\n\n\n\n\n Mapping Operation \n\n\n\n\n\n\n\n\n***********")
    row_permutation = matrix.mapPartitions(lambda row: multiply(row))
    #print(row_permutation.collect())
    print("**************\n\n\n\n\n\n\n Reduce Operation \n\n\n\n\n\n\n\n\n***********")
    row_permutation = row_permutation.reduce(add)
    #print(row_permutation)
    print(row_permutation.shape)
    print(type(row_permutation))
    #print(row_permutation[:,0])

    ris=matrix.map(lambda line: list(np.dot(line,row_permutation)))
    print(len(ris.collect()))
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