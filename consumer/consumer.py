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
        data.append((reg[1], reg[2]))
    return data


dataJson = socket_stream.map(convertToJSON)
pairs = dataJson.map(splitEntry)

pairs.pprint()


# movieRatings = interactions.map(lambda x: (int(x.split('::')[1]), [float(x.split('::')[2]),1]))
#     totalOfMovieRatings = movieRatings.reduceByKey(lambda x, y: [x[0] + y[0],x[1]+y[1]])

# res = pairs.reduceByKey(lambda a, b: a+b)

# res.pprint()

# ratings = ssc.textFileStream("../database/").map
# ratings.pprint()

ssc.start()

ssc.awaitTermination()
# WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.
# WARN BlockManager: Block input-0-1653087338600 replicated to only 0 peer(s) instead of 1 peers
