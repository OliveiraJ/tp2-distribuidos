from pprint import pprint
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel


sc = SparkContext()
ssc = StreamingContext(sc, 15)

socket_stream = ssc.socketTextStream(
    "127.0.0.1", 4458, storageLevel=StorageLevel(True, True, False, False, 1))


def convertToJSON(entry):
    return json.loads(entry)


count = socket_stream.map(convertToJSON).map(
    lambda x: (str(x[1]), 1)).reduceByKey(lambda a, b: a+b)

pairList = socket_stream.map(convertToJSON).map(
    lambda x: (str(x[1]), float(x[2]))).reduceByKey(lambda a, b: a+b)

top_ten = pairList.join(count).map(lambda x: (x[0], round(
    x[1][0]/x[1][1], 2))).map(lambda x: (x[1], x[0])).transform(lambda x: x.sortByKey(False))


top_ten.pprint(10)

ssc.start()

ssc.awaitTermination()
