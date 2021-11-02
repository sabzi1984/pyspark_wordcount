#mapper word count
#used the following source: https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
import sys
def wordcount_mapper():
  for line in sys.stdin:
      line = line.strip()
      words = line.split()
      for word in words:
          yield tuple(word,'\t', 1)

          
if __name__ == "__main__":
    wordcount_mapper()