
import csv

file_name = "data/zmp3.csv"

from pymongo import MongoClient
client = MongoClient()
client = MongoClient('0.0.0.0', 2701)

db = client.test

saved = []

with open(file_name, "rb") as meta_file :
    reader = csv.reader(meta_file, delimiter=',', quotechar='"')
    for line in reader :

        if line[0] in saved :
            continue

        saved.append(line[0])

        data = {}
        data['_id'] = line[0]
        data['title'] =line[1]
        data['artists'] = line[2]
        data['composers'] = line[3]
        data['album'] = line[4]
        data['genre'] = line[5]

        posts = db.metaData.insert_one(data)
