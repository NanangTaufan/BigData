from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    parkZipCode = 0
    count = 1
    if(fields[7] != ''):
        parkZipCode = int(fields[7])
    return (parkZipCode, count)
    
lines = sc.textFile("C:/spark/dataset/dataset.csv")
rdd = lines.map(parseLine)
totalByparkZipCode = rdd.filter(lambda x: x > 0).reduceByKey(lambda x, y: x + 1).sortByKey()

for result in totalByparkZipCode.collect():
    print result
