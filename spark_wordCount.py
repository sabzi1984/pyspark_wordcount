# import os
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import time


spark = SparkSession.builder.getOrCreate()

def map_words(row):

    words = row[0].split(' ') 
    for w in words:
        yield (w, 1)

def reduce_words(value1, value2):
    return value1+value2

    
exec_time_list=[]
book_list=[]


urls=['http://www.gutenberg.ca/ebooks/buchanj-midwinter/buchanj-midwinter-00-t.txt', 'http://www.gutenberg.ca/ebooks/carman-farhorizons/carman-farhorizons-00-t.txt','http://www.gutenberg.ca/ebooks/colby-champlain/colby-champlain-00-t.txt',
      'http://www.gutenberg.ca/ebooks/cheyneyp-darkbahama/cheyneyp-darkbahama-00-t.txt','http://www.gutenberg.ca/ebooks/delamare-bumps/delamare-bumps-00-t.txt',
      'http://www.gutenberg.ca/ebooks/charlesworth-scene/charlesworth-scene-00-t.txt','http://www.gutenberg.ca/ebooks/delamare-lucy/delamare-lucy-00-t.txt',
      'http://www.gutenberg.ca/ebooks/delamare-myfanwy/delamare-myfanwy-00-t.txt','http://www.gutenberg.ca/ebooks/delamare-penny/delamare-penny-00-t.txt']
book_titles=['buchanj-midwinter-00-t.txt','carman-farhorizons-00-t.txt','colby-champlain-00-t.txt','cheyneyp-darkbahama-00-t.txt','delamare-bumps-00-t.txt',
            'charlesworth-scene-00-t.txt','delamare-lucy-00-t.txt','delamare-myfanwy-00-t.txt','delamare-penny-00-t.txt']
spark = SparkSession.builder.getOrCreate()
start_add_file_time = time.time()
for j in range(len(urls)):
  spark.sparkContext.addFile(urls[j])
end_add_file_time = time.time()

start_data_read = time.time()
for i in range(len(book_titles)):

  book_list.append(spark.read.csv("file://"+SparkFiles.get(book_titles[i]), header=False, inferSchema= True) )
end_read_time = time.time()
for _ in range(3):
  start = time.time()
  for i in range(len(book_titles)):

    # book = spark.read.csv("file://"+SparkFiles.get(book_titles[i]), header=False, inferSchema= True)
    # print(book1_rdd.collect())
    book_rdd = book_list[i].rdd

    book_rdd = book_rdd.flatMap(map_words)
    # print(book1_rdd.collect())

    book_rdd = book_rdd.reduceByKey(reduce_words)
    # print(book1_rdd.collect())

    # book_rdd.toDF(["word", "count_word"]).show()


  end = time.time()
  exec_time=end-start
  exec_time_list.append(exec_time)
reading_data_time=end_read_time-start_data_read
adding_data_time=end_add_file_time-start_add_file_time

print(f'MapReduce Time:{exec_time_list}')
print(f'Reading Data Time :{reading_data_time}')
print(f'add Data Time :{adding_data_time}')