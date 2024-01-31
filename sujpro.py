from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
#
# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'sony'

# CSV file path
csv_file_path = '/home/ubh01/apple_quality.csv'

# Define the schema for the CSV file
schema = StructType([
    StructField("_c0", StringType(), True),
    StructField("_c1", StringType(), True),
    # Add more fields as per your CSV file structure
])

# Create a Spark session
spark = SparkSession.builder.appName("StructuredStreamingKafkaIntegration").getOrCreate()

# Read CSV file in a streaming fashion with specified schema
csv_stream_df = (
    spark
    .readStream
    .schema(schema)
    .csv(csv_file_path, header=True)
    .selectExpr("CAST(_c0 AS STRING) as key", "CAST(_c1 AS STRING) as value")
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
