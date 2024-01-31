from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'sony'

# CSV file path
csv_file_path = '/home/ubh01/Downloads/apple_quality.csv'

# Create a Spark session
spark = SparkSession.builder.appName("StructuredStreamingKafkaIntegration").getOrCreate()

# Read CSV file in a streaming fashion
csv_stream_df = (
    spark
    .readStream
    .csv(csv_file_path, header=True)
    .selectExpr("CAST(_c0 AS STRING) as key", "CAST(_c1 AS STRING) as value")  # Assuming the first column is the key and the second column is the value
)

# Define a function to send messages to Kafka
def send_to_kafka(df, epoch_id):
    kafka_df = df.limit(1)  # Limit to one row per micro-batch
    kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", kafka_topic) \
            .save()

# Start the streaming query
streaming_query = (
    csv_stream_df
    .writeStream
    .foreachBatch(send_to_kafka)
    .trigger(processingTime="5 seconds")  # Trigger every 5 seconds
    .start()
)

# Await termination of the streaming query
streaming_query.awaitTermination()
