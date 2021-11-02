#mapper word count
# used the following source: https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
from itertools import groupby
import sys

def wordcount_reducer():
    for line in sys.stdin:
        data=line.rstrip().split('\t', 1)
        for current_word, group in groupby(data, lambda x: x[0]):
            total_count = sum(int(count) for current_word, count in group)
            yield(tuple(current_word, '\t', total_count)) 


if __name__ == "__main__":
    wordcount_reducer()