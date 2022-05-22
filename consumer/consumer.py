from pprint import pprint
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel


sc = SparkContext()
ssc = StreamingContext(sc, 15)

# connects to the server and starts to receive data from the producer
socket_stream = ssc.socketTextStream(
    "127.0.0.1", 4458, storageLevel=StorageLevel(True, True, False, False, 1))

# converts the received data back to json format to make it easier to split and work with the values


def convertToJSON(entry):
    return json.loads(entry)


# this DStream is responsible for the count of how many times an movies appers at the data
count = socket_stream.map(convertToJSON).map(
    lambda x: (str(x[1]), 1)).reduceByKey(lambda a, b: a+b)
# this DStream is responsible to sum all the ratings a movie has received
pairList = socket_stream.map(convertToJSON).map(
    lambda x: (str(x[1]), float(x[2]))).reduceByKey(lambda a, b: a+b)
# this DStream joins the two other aboves and returns the data with the median of the ratins and sorted as requested
top_ten = pairList.join(count).map(lambda x: (x[0], round(
    x[1][0]/x[1][1], 2))).map(lambda x: (x[1], x[0])).transform(lambda x: x.sortByKey(False))


top_ten.pprint(10)

ssc.start()

ssc.awaitTermination()
