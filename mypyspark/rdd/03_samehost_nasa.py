from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("same_host").setMaster("local")
sc = SparkContext(conf=conf)

julyFirstLogs = sc.textFile("/Users/iamsuman/src/iamsuman/myspark/mypyspark/data/nasa_19950701.tsv")
augFirstLogs = sc.textFile("/Users/iamsuman/src/iamsuman/myspark/mypyspark/data/nasa_19950801.tsv")


julyFirstLogs = julyFirstLogs.map(lambda line: line.split("\t")[0])
augFirstLogs = augFirstLogs.map(lambda line: line.split("\t")[0])

intersection = julyFirstLogs.intersection(augFirstLogs)
cleanedHostIntersection = intersection.filter(lambda host: host != "host")
cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv")
