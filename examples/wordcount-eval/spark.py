#!/usr/bin/python3

from pyspark import SparkContext, SparkConf
import sys

if __name__ == "__main__":

    parallelism = sys.argv[2]
        
    # create Spark context with necessary configuration
    sc = SparkContext("local[{}]".format(parallelism),
                      "Wordcount")

    # read data from text file and split each line into words
    words = sc.textFile(sys.argv[1]) \
              .flatMap(lambda line: line.split(" ")) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a + b, int(parallelism))

    for count in words.collect():
        print("{} {}".format(count[0].encode('utf-8'), count[1]))
