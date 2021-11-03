import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import time
import sys
import csv



def map_words(row):

    words = row[0].split(' ') 
    for w in words:
        yield (w, 1)

def reduce_words(value1, value2):
    return value1+value2

try:
    if (sys.argv[1] == '-'):
        f = sys.stdin.read()
    else:
        filename = sys.argv[1]
        f = open(filename, 'r')
    book = csv.reader(f)

spark = SparkSession.builder.getOrCreate()
# book = spark.read.csv(sys.stdin, header=False)
# start_add_file_time = time.time()

book_rdd = book.rdd

book_rdd = book_rdd.flatMap(map_words)
# print(book1_rdd.collect())

book_rdd = book_rdd.reduceByKey(reduce_words)
# print(book1_rdd.collect())

book_rdd.toDF(["word", "count_word"]).show()
# yield(book_rdd.toDF(["word", "count_word"]))