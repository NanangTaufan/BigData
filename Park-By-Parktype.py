from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Count-Park-By-ParkType")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    parkType = str(fields[1])
    count = 1
    return (parkType, count)
    
lines = sc.textFile("C:/spark/dataset/dataset.csv")
rdd = lines.map(parseLine)
totalByparkType = rdd.reduceByKey(lambda x, y: x + 1).sortByKey()

for result in totalByparkType.collect():
    print result
