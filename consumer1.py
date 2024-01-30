from kafka import KafkaConsumer
from pydoop import hdfs

TOPIC_NAME='KafkaInPython'

consumer =KafkaConsumer(TOPIC_NAME)
hdfs_path='hdfs://localhost:9000//KafkaPython/message.txt'

for message in consumer:
    values=message.value
    with hdfs.open(hdfs_path, 'a') as wfile:
        wfile.write(values)
