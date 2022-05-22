
import time
import socket
import json
import numpy as np


FILE_PATH = "../ml-1m/ratings.dat"
SAMPLE_SIZE = 200000


def loadSample(filePath: str, qtdSamples: int):
    samplelist = []

    with open(filePath, "rb") as file:
        read = file.read().splitlines()

        # sample = random.choices(read, k=qtdSamples)

        sample = np.random.choice(read, qtdSamples)
        for s in sample:
            s = str(s)
            data = s.split('::')
            data[0] = data[0].replace("b'", "")
            data[2] = int(data[2])
            data[3] = data[3].replace("'", "")
            samplelist.append(data)

    return samplelist


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

    data = open(FILE_PATH).read().splitlines()

    print("Listening on port: %s..." % str(port))

    s.listen()
    c, addr = s.accept()

    print("Received request from: " + str(addr))

    data = loadSample(FILE_PATH, SAMPLE_SIZE)
    while True:

        for line in data:
            lineJson = json.dumps(line)
            c.send(bytes((lineJson+'\n').encode('utf-8')))

        print("New sample written")

        time.sleep(15)


if __name__ == "__main__":
    main()
