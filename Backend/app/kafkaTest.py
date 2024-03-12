from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType
from pyspark.sql.save import SaveMode
from confluent_kafka import Producer
import json

def write_partitioned_data_to_kafka(spark, df, kafka_bootstrap_servers, kafka_topic):
  """Writes a partitioned Spark DataFrame to Kafka topic with Confluent Kafka.

  Args:
      spark (SparkSession): SparkSession object.
      df (pyspark.sql.DataFrame): DataFrame containing partitioned data.
      kafka_bootstrap_servers (str): Comma-separated list of Kafka bootstrap servers.
      kafka_topic (str): Name of the Kafka topic to write to.
  """

  # Define Kafka producer configuration
  producer_config = {
      "bootstrap.servers": kafka_bootstrap_servers,
      "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
  }

  # Write each partition to Kafka topic with partition key set
  df.write \
     .format("kafka") \
     .options(**producer_config) \
     .option("topic", kafka_topic) \
     .partitionBy("partition_col") \
     .saveMode(SaveMode.Append)  # Use Append or Overwrite as needed

  print("Data successfully written to Kafka topic:", kafka_topic)

def read_and_partition_csv(spark, csv_path, num_partitions, schema=None):
  """Reads a large CSV file with PySpark, partitions it, and returns a DataFrame.

  Args:
      spark (SparkSession): SparkSession object.
      csv_path (str): Path to the CSV file.
      num_partitions (int): Number of partitions to create.
      schema (StructType, optional): CSV schema if known. Defaults to None.

  Returns:
      pyspark.sql.DataFrame: Partitioned DataFrame containing the CSV data.
  """

  # Set number of partitions for efficient processing of large files
  spark.conf.set("spark.sql.shuffle.partitions", num_partitions)

  # If schema is not provided, infer it from the CSV
  if not schema:
    schema = spark.read.csv(csv_path, header=True).schema

  # Read CSV data into a DataFrame with specified schema
  df = spark.read.csv(csv_path, header=True, schema=schema)

  return df

if __name__ == "__main__":
  # Create SparkSession with adjusted default parallelism (optional)
  spark = SparkSession.builder.appName("CSVToKafkaProducer").getOrCreate()
  spark.conf.set("spark.default.parallelism", num_executors * num_cores)  # Adjust for your cluster

  # Replace with your actual values
  csv_path = "path/to/your/huge.csv"
  num_partitions = 100  # Adjust based on your cluster and data size
  schema = None  # Or provide your CSV schema if known
  kafka_bootstrap_servers = "localhost:9092"
  kafka_topic = "my_kafka_topic"

  # Read and partition the CSV data
  df = read_and_partition_csv(spark, csv_path, num_partitions, schema)

  # Write partitioned data to Kafka
  write_partitioned_data_to_kafka(spark, df, kafka_bootstrap_servers, kafka_topic)

  spark.stop()
