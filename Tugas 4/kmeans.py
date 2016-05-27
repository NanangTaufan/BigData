from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import sys

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    Lt = float(fields[13])
    Lng = float(fields[14])
    return (Lt, Lng)

# Load and parse the data
data = sc.textFile("E:/BigData/Dataset-lama.csv")
parsedData = data.map(parseLine)


NumCluster = int(sys.argv[1])

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, NumCluster, maxIterations=10, runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    print center
    
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)