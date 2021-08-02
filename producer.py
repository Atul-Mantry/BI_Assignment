from kafka import KafkaProducer
import json
import logging
import os
import time

#logging.basicConfig(level=logging.DEBUG)
#logging.basicConfig(level=logging.ERROR)

#defining kafka K8 load balancers
#bootstrap_servers = ['10.164.0.2:9094'] # issue with this server
bootstrap_servers = ['10.164.0.4:9094','10.164.0.3:9094']

file_type = '*.json'
file_path = r'/home/atulmantry1992/scripts/gcs_bucket'  #path should be mounted first

#target topic name
topicName = 'UserData'

#defining producer
producer = KafkaProducer(bootstrap_servers = bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'),api_version=(2, 7, 1))

#fetching all json files
json_files = [pos_json for pos_json in os.listdir(file_path) if pos_json.endswith('.json')]

#making completion
print("Checking files to process")

#fetching data line by line from each json file and publishing to topic
if (len(json_files) >0):
        for file in json_files:
                full_file = file_path + '/' + file
                with open(full_file) as f:
                        for lines in f:
                            a = json.loads(lines)
                            producer.send(topicName,a)
                            print(lines)
                            time.sleep(4)

							
else:
        print("No file to process")

#making completion
print("Exiting process")
                         