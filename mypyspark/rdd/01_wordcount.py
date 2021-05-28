
from pyspark import SparkContext, SparkConf
import sys

if __name__ == "__main__":
    conf = SparkConf().setAppName("word_count").setMaster("local[3]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    lines = sc.textFile("/Users/iamsuman/src/iamsuman/myspark/mypyspark/data/word_count.txt")

    words = lines.flatMap(lambda line: line.split(" "))
    wordcounts = words.countByValue()

    for word, count in wordcounts.items():
        print(f"{word}: {count}")




