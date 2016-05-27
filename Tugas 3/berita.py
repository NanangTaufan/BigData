from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import sys

# Function for printing each element in RDD
def println(x):
    print x

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents (one per line).
rawData = sc.textFile("E:/BigData/daftar-berita.tsv")
fields = rawData.map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[3].lower().split(" "))

documentId = fields.map(lambda x: x[0])


hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(documents)

tf.cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)

keyword = str(sys.argv[1])
keywordTF = hashingTF.transform([keyword.lower()])
#keywordTF = hashingTF.transform(["anarchism"])
keywordHashValue = int(keywordTF.indices[0])

keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

zippedResults = keywordRelevance.zip(documentId)
tupleResult = tuple(zippedResults.max())
idResult = str(tupleResult[1])

print "\nBest document for keyword '" + keyword + "' is id = " + idResult
#print zippedResults.max()