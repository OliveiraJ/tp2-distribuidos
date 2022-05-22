from pprint import pprint
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel

sc = SparkContext()
ssc = StreamingContext(sc, 10)

socket_stream = ssc.socketTextStream(
    "127.0.0.1", 4458, storageLevel=StorageLevel(True, True, False, False, 1))


def convertToJSON(entry):
    return json.loads(entry)


def splitEntry(entry):
    data = list()
    for reg in entry:
        data.append((reg[1], int(reg[2])))
    return data


count = socket_stream.map(convertToJSON).map(
    lambda x: (str(x[1]), 1)).reduceByKey(lambda a, b: a+b)

pairList = socket_stream.map(convertToJSON).map(
    lambda x: (str(x[1]), float(x[2]))).reduceByKey(lambda a, b: a+b)


pairList.pprint()

count.pprint()


ssc.start()

ssc.awaitTermination()
