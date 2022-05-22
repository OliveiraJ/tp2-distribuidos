
import time
import socket
import json
import numpy as np

# path to the file from where the data is readed
FILE_PATH = "../ml-1m/ratings.dat"
# the size of the sample we will send to the cosumer
SAMPLE_SIZE = 200000

# Function tha loads random data from the ratings.dat file and also cleans it returning a list of lists


def loadSample(filePath: str, qtdSamples: int):
    samplelist = []

    with open(filePath, "rb") as file:
        read = file.read().splitlines()

        sample = np.random.choice(read, qtdSamples)
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

    data = loadSample(FILE_PATH, SAMPLE_SIZE)

    # sends the colected data to the consumer at every 15s, here all line are sent on by one
    while True:
        for line in data:
            # converts every line to a json format that later will be converted to to bytes and then sent to the consumer
            lineJson = json.dumps(line)
            c.send(bytes((lineJson+'\n').encode('utf-8')))

        print("New sample written")

        time.sleep(15)


if __name__ == "__main__":
    main()
