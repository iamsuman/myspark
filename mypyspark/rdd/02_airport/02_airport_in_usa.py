from pyspark import SparkContext, SparkConf
from mypyspark.commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])


if __name__ == "__main__":
    conf = SparkConf().setAppName("airprts_in_usa").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    # sc.setLogLevel("ERROR")

    airports = sc.textFile("/Users/iamsuman/src/iamsuman/myspark/mypyspark/data/airports.text")
    airportsInUSA = airports.filter(lambda line: line.split(",")[3] == "\"United States\"")

    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")