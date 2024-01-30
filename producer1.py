from kafka import KafkaProducer

TOPIC_NAME='KafkaInPython'
KAFKA_SERVER=['localhost:9092']

producer=KafkaProducer(bootstrap_servers=KAFKA_SERVER)

producer.send(Topic_Name,b'Hello this is kafka in pyhton')
producer.flush()
