# Distributed Data Infrastructures, Fall 2018, Project 1

# Assignment
In this project, you are supposed to use Apache Spark to analyze a large data set. Spark is an open source big data framework which we have discussed in the course. We provide you with two data sets and you should run Spark on Ukko2 to answer one question about each data set.

# Requirements
You need write a program that uses Spark to provide an answer to the following questions.
1. For the first data set (data-1.txt), provide the value of the median of the data set? You should provide the exact median, not an approximation, and sorting the complete data set is not an acceptable solution.
2. The second data set (data-2.txt) contains the matrix A. Your task is to calculate A x AT x A and provide the resulting matrix as your answer in the same format as the input matrix.

The data-1.txt is in the following format.

3.01316363

16.41347991

11.73966247

74.71116433

29.53299636

5.91881846

21.12204071

...

The file has one billion rows and each row contains only one float number.

data-2.txt is text file containing a 1000000 x 1000 matrix. The file is stored in the text format, and each line represents a row vector. The row contains 1000 float numbers which are separated by white-spaces.

# Documentation
It can be found in the file "project1_documentation.pdf".
