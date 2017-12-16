
from __future__ import division
import csv
import json

# map = {}
#
# with open ('data/zmp3.csv', 'rb') as meta_file:
#     reader =  csv.reader(meta_file, delimiter=',', quotechar='"')
#
#     for row in reader :
#         id = row [0]
#         name = row [1]
#         map[id] = name

time_threshold = 0.2
files_num = 10




line_n = 0

total_data = 126983006

batch =  100000


for num in range(files_num) :

    user_data_map = {}
    for line in open('data/zmp3_log.txt'):
        data = line.split('\t')

        if len(data) < 5:
            continue

        user_str = data[0]
        song_str = data[1]
        startTime_str = data[2]
        duration_str = data[3]
        song_length = data[4]

        if int(user_str) % files_num != num:
            continue

        line_n += 1
        if line_n % batch == 0:
            print line_n / batch

        if 'null' not in song_length:
            int_part = song_length[0:song_length.index(".")]

            duration_int = int(duration_str)
            song_length_int = int(int_part)

            if song_length_int > 0 :
                if duration_int / song_length_int < time_threshold:
                    continue


        if user_str not in user_data_map:
            user_data_map[user_str] = {}

        user_in_map = user_data_map[user_str]

        if song_str not in user_in_map :
            user_in_map[song_str] = []

        song_in_user = user_in_map[song_str]

        song_in_user.append({
            'startTime' : startTime_str,
            'duration' : duration_str
        })

    with open('output' + str(num) + '.txt', 'w') as outfile:
        json.dump(user_data_map, outfile)


