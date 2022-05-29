
import time
import socket
import numpy as np
import random
import pickle

# path to the file from where the data is readed
FILE_PATH = "../ml-1m/ratings.dat"

# the size of the sample we will send to the cosumer
SAMPLE_SIZE = 100
BUFFER_SIZE = 1024

HEADERSIZE = 10

# Function tha loads random data from the ratings.dat file and also cleans it returning a list of lists


def loadSample(filePath: str, qtdSamples: int):
    samplelist = []

    with open(filePath, "rb") as file:
        read = file.read().splitlines()

        sample = random.choices(read, k=qtdSamples)
        for s in sample:
            s = str(s)
            data = s.split('::')
            data[0] = data[0].replace("b'", "")
            data[2] = int(data[2])
            data[3] = data[3].replace("'", "")
            samplelist.append(data)

    return samplelist


def main():
    # creates the socket/tcp server
    s = socket.socket()
    host = "127.0.0.1"
    port = 4458
    s.bind((host, port))

    print("Listening on port: %s..." % str(port))

    s.listen()
    c, addr = s.accept()

    print("Received request from: " + str(addr))

    # sends the colected data to the consumer at every 15s, here all line are sent on by one
    while True:
        data = loadSample(FILE_PATH, SAMPLE_SIZE)
        # converts every line to a json format that later will be converted to to bytes and then sent to the consumer
        lines = pickle.dumps(data)
        c.send(bytes(f"{len(lines):<{HEADERSIZE}}", 'utf-8')+lines)
        # c.send(bytes((data+'\n').encode('utf-8')))
        print("New sample written")

        time.sleep(10)


if __name__ == "__main__":
    main()
