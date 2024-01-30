import csv
import requests
from kafka import KafkaProducer
from json import dumps

TOPIC_NAME = "StockData"
KAFKA_SERVER = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda K: dumps(K).encode('utf-8'))

API_KEY = "2OWSX3KJQKVEWV6D"
CSV_URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=1min&slice=year1month1&apikey={API_KEY}"

with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    header = next(cr)  # Get the header
    my_list_of_dicts = []

    for row in cr:
        data_dict = dict(zip(header, row))
        my_list_of_dicts.append(data_dict)
        producer.send(TOPIC_NAME, data_dict)
        print(data_dict)  # For testing, print each dictionary

producer.close()  # Close the producer after sending data
'''
import csv
import requests
from kafka import KafkaProducer
from json import dumps

TOPIC_NAME = "StockData"
KAFKA_SERVER = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda K: dumps(K).encode('utf-8'))

API_KEY = "2OWSX3KJQKVEWV6D"
CSV_URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=1min&slice=year1month1&apikey={API_KEY}"

with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    header = next(cr)  # Get the header

    for row in cr:
        data_dict = dict(zip(header, row))
        producer.send(TOPIC_NAME, data_dict)
        print(data_dict)  # Print each dictionary as it is processed

producer.close()  # Close the producer after sending data
'''
