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



# Import libraries
import kfp

# Define pipeline parameters (replace with your parameters)
name_param = kfp.components.inputs.Argument(type=str, default="example-pipeline")

# Define placeholder functions for pipeline steps (replace with your functions)
def data_preprocess(data_path):
  # Preprocess data at 'data_path'
  print(f"Preprocessing data from {data_path}")
  # ... (your data preprocessing logic here)
  return "preprocessed_data"

def train_model(data):
  # Train model using 'data'
  print(f"Training model with data")
  # ... (your model training logic here)
  return "trained_model"

# Create KFP components from functions
data_preprocess_op = kfp.components.create_component_from_func(
    data_preprocess,
    base_image="python:3.7",
    output_artifacts=["preprocessed_data"]
)

train_model_op = kfp.components.create_component_from_func(
    train_model,
    base_image="tensorflow:2.4-gpu",
    input_artifacts=["preprocessed_data"],
    output_artifacts=["trained_model"]
)

# Build the pipeline
@kfp.dsl.pipeline
def my_pipeline(data_path=name_param):
  preprocessed_data = data_preprocess_op(data_path)
  trained_model = train_model_op(preprocessed_data.output)

# Compile the pipeline
pipeline_definition = kfp.compiler.PipelineCompiler().compile(my_pipeline)

# Run the pipeline (replace with your Kubeflow Pipelines API endpoint)
client = kfp.Client(endpoint="http://kubeflow-pipeline-api:8888")
client.create_run_from_pipeline_func(pipeline_definition)

print("Pipeline submitted successfully!")
