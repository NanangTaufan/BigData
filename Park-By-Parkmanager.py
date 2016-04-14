from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    parkManager = str(fields[3]).replace('"','')
    count = 1
    return (parkManager, count)
    
lines = sc.textFile("C:/spark/dataset/dataset.csv")
rdd = lines.map(parseLine)
totalByparkManager = rdd.reduceByKey(lambda x, y: x + 1).sortByKey()

for result in totalByparkManager.collect():
    print result
