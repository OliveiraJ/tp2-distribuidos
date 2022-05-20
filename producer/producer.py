import random
import time
import csv


FILE_PATH = "./ml-1m/ratings.dat"
SAMPLE_SIZE = 200000


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


def writeFile(data):
    header = ['user_id', 'movie_id', 'rating', 'time_stamp']
    with open("./database/evaluations.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        writer.writerows(data)


def main():
    while True:
        data = loadSample(FILE_PATH, SAMPLE_SIZE)
        writeFile(data)
        print("New sample written")
        time.sleep(5)


if __name__ == "__main__":
    main()
