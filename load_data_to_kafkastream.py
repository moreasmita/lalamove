import sys
import time
from pykafka import KafkaClient

DataFilePath = "data.json"
KafkaHost = "35.184.206.128:9092"
TopicName = "customerlogs"

client = KafkaClient(hosts=KafkaHost)

Topic = client.topics[TopicName]

try:
    with open(DataFilePath,'r') as datafile:
        counter = 0
        for line in datafile:
             counter +=1
             with Topic.get_sync_producer() as producer:
                 print(line)
                 message = str.encode(line)
                 producer.produce(line)
             if counter == 100:
                print("waiting for 10 seconds...")
                counter = 0
                time.sleep(1)
except OSError as e:
    print(e)
