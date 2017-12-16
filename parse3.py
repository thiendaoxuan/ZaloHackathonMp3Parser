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
file_name = 'output' + str(i) + '.txt'

data = json.load(open(file_name))

print "Total SIze : " + str(len(data))

holder = []

for username in data:

    count += 1



    if count%print_threshold == 0:
        print count
        posts = db.zmp3.insert_many(holder)
        holder = []

    json_data = data[username]

    to_insert = {}
    to_insert['_id'] = username

    to_insert['songListen'] = json_data
    to_insert['songRecommend'] = {}

    holder.append(to_insert)

