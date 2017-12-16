import json
import gc

from pymongo import MongoClient
client = MongoClient()
client = MongoClient('0.0.0.0', 2701)

db = client.test

total_files = 10

print_threshold = 1000

count = 0

i = 9

gc.collect()
# file_name = 'data/simple.txt'
file_name = 'output' + str(i) + '.txt'

data = json.load(open(file_name))
print "Total SIze : " + str(len(data))

holder = []
for username in data:

    count += 1

    if count%print_threshold == 0 or count == len(data)-1:
        print count
        posts = db.training_zmp3.insert_many(holder)
        holder = []


    json_data = data[username]

    for song in json_data:
        to_insert = {
            'userId' : username,
            'songId' : song,
            'count' : len(json_data[song])
        }

        holder.append(to_insert)


