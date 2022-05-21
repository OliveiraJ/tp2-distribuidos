import random
import time
import csv
import socket
import json


FILE_PATH = "../ml-1m/ratings.dat"
SAMPLE_SIZE = 20


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

        resJson = json.dumps(samplelist)
    return resJson


# def writeFile(data):
#     header = ['user_id', 'movie_id', 'rating', 'time_stamp']
#     with open("../database/evaluations"+time.strftime("%d%b%Y%H%M%S", time.gmtime())+".csv", "w", newline='') as f:
#         writer = csv.writer(f)
#         writer.writerow(header)

#         writer.writerows(data)


def main():
    s = socket.socket()
    host = "127.0.0.1"
    port = 4458
    s.bind((host, port))

    print("Listening on port: %s..." % str(port))

    s.listen()
    c, addr = s.accept()

    print("Received request from: " + str(addr))

    while True:
        data = loadSample(FILE_PATH, SAMPLE_SIZE)
        # writeFile(data)
        print("New sample written")
        c.send(bytes((data+'\n').encode('utf-8')))
        time.sleep(10)


if __name__ == "__main__":
    main()
