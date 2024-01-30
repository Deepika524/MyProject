import csv

import requests

import os

from kafka import KafkaProducer

from json import dumps

from dotenv import load_dotenv

load_dotenv('.env')

bootstrap_servers=['localhost:9092']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda K:dumps(K).encode('utf-8'))

demo = os.getenv('API_KEY')

#replace the "demo" apikey below with your own key from https://www.alphavantage.co/suppines SV_URL = 'https://www.alphavantage.co/query?function=TIME SERIES INTRADAY EXTENDED&G)

CSV_URL="https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDES&symbol=IBM&interval=5min&slice=year1month1&apikey={}".format(demo)
with requests. Session() as s:

    download = s.get(CSV_URL)
    
    decoded_content = download.content.decode('utf-8')

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')

    my_list = list(cr)

    for row in my_list:

        producer.send('text',row)
