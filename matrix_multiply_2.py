from pyspark import SparkConf, SparkContext
import sys, operator




def add_tuples(a, b):
    return list(sum(p) for p in zip(a,b))
	
def permutation(row):
	rowPermutation = []
	
	for element in row:
		for e in range(len(row)):
			rowPermutation.append(float(element) * float(row[e]))
			
		
	
	return rowPermutation
	


def main():
	dataset = "stupido.txt"
	output = "prova2.txt"

	conf = (SparkConf()
            .setAppName("genchi")           ##change app name to your username
            .setMaster("spark://128.214.48.227:7077")
            # .setMaster("local")
            .set("spark.cores.max", "40")  ##dont be too greedy ;)
            .set("spark.rdd.compress", "true")
            .set("spark.broadcast.compress", "true")
           )

	sc = SparkContext(conf=conf)
	
	row = sc.textFile(dataset).map(lambda line: line.split()).map(lambda value: [float(i) for i in value]).cache()
	ncol = len(row.take(1)[0])
	intermediateResult = row.map(permutation).reduce(add_tuples)
	
	outputFile = open(output, 'w') 
	
	result = [intermediateResult[x:x+3] for x in range(0, len(intermediateResult), ncol)]
	
	
	for row in result:
		for element in row:
			outputFile.write(str(element) + ' ')
		outputFile.write('\n')
		
	outputFile.close()
	
	# outputResult = sc.parallelize(result).coalesce(1)
	# outputResult.saveAsTextFile(output)

	
	
	
if __name__ == "__main__":
	main()