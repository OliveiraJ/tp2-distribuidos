
import numpy as np

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import socket
import pickle
import time


FILE_PATH = "../ml-1m/movies.dat"
HEADERSIZE = 10


sc = SparkContext()


def loadMovies(filePath: str):
    movielist = []

    with open(filePath, encoding='ISO-8859-1') as file:
        read = file.read().splitlines()

        for s in read:
            s = str(s)
            data = s.split('::')
            data[0] = data[0].replace("b'", "")
            data[1] = data[1]
            data[2] = data[2].replace("'", "")
            movielist.append(data)

    return movielist


def createRDD(data):
    return sc.parallelize(data)


def main():
    host = "127.0.0.1"
    port = 4458

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))

    def doMedia(x):
        return round((x[0]/x[1]), 2)

    movies = loadMovies(FILE_PATH)

    def runSpark(list):

        rdd = createRDD(list)
        result = rdd.map(lambda x: (x[1], [float(x[2]), 1])
                         ).reduceByKey(lambda x, y: [x[0]+y[0], x[1]+y[1]]).mapValues(doMedia).map(lambda x: (x[1], x[0])).sortByKey(False)
        top_ten = result.take(10)

        return top_ten

    while True:
        list = []
        data = b''
        new_msg = True

        while True:
            res = client.recv(80192)

            if new_msg:
                msglen = int(res[:HEADERSIZE])
                new_msg = False

            data += res

            if len(data)-HEADERSIZE == msglen:
                list = pickle.loads(data[HEADERSIZE:])
                new_msg = True
                data = b""

            top_ten = runSpark(list)

            for reg in top_ten:
                for movie in movies:
                    if(reg[1] == movie[0]):
                        print(movie[1], ",", reg[0])

            print("\n")


if __name__ == "__main__":
    main()
