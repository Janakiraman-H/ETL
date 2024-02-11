import threading
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from kafka import KafkaProducer, KafkaConsumer
import json
from pymongo import MongoClient

def produce_to_kafka():
    # Producer logic here
    spark = SparkSession.builder \
        .appName("CSV to Kafka Streaming Producer") \
        .getOrCreate()

    schema = StructType().add(...)  # Define your schema
    csv_path = "path/to/your/largefile.csv"

    streaming_df = spark.readStream.option("header", "true").schema(schema).csv(csv_path)

    producer = KafkaProducer(bootstrap_servers='<kafka-broker>', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_to_kafka(row):
        message = row.asDict()
        producer.send('<kafka-topic>', value=message)

    query = streaming_df.writeStream.foreach(send_to_kafka).start()
    query.awaitTermination()

    spark.stop()
    producer.close()

def consume_and_write_to_mongodb():
    # Consumer logic here
    spark = SparkSession.builder \
        .appName("Kafka to MongoDB Streaming Consumer") \
        .getOrCreate()

    schema = StructType().add(...)  # Define your schema

    consumer = KafkaConsumer('<kafka-topic>',
                             bootstrap_servers='<kafka-broker>',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    client = MongoClient('mongodb://<username>:<password>@<host>:<port>/')
    db = client['<database>']
    collection = db['<collection>']

    for message in consumer:
        kafka_data = message.value
        row = spark.createDataFrame([kafka_data], schema=schema)
        row.write.format("mongo").mode("append").save()

    spark.stop()

# Run producer and consumer in parallel
producer_thread = threading.Thread(target=produce_to_kafka)
consumer_thread = threading.Thread(target=consume_and_write_to_mongodb)

producer_thread.start()
consumer_thread.start()

# Sleep for some time to allow parallel processing
sleep(600)  # Adjust the sleep time based on your requirements

# Join threads to wait for their completion
producer_thread.join()
consumer_thread.join()
