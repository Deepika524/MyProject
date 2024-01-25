from kafka import KafkaConsumer

import sys

import json

import os

from kafka.consumer import group

from dotenv import load_dotenv

load_dotenv('.env')

key = os.getenv('bootstrap_servers')

consumer = KafkaConsumer('python-kafka-stock-txt',auto_offset_reset = 'earliest',group_id = None)
#creating another file to store the data

sys.stdout-open('/home/lenovo/Desktop/Python_work/hadoop/KAFKA/stockdata/stockdataconsole.txt','w')

for message in consumer:

    values = message.value

    print(values)

sys.stdout.close()
