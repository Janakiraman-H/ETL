from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct

def upsert_to_mongo(data_list):
  """
  Upserts data from a list of dictionaries to MongoDB using PySpark.

  Args:
      data_list (list): A list of dictionaries containing data for upserts.
  """
  spark = SparkSession.builder.getOrCreate()

  # Create a Spark DataFrame from the list of dictionaries
  data_df = spark.createDataFrame(data_list)

  # Assuming data has an _id column (adapt as needed)
  processed_data = data_df.withColumn("update_doc", your_update_logic(col("data")))  # Replace with your logic

  # Create DataFrame for upsert (adapt column names as needed)
  upsert_data = processed_data.select(col("_id"), 
                                       struct(lit("_id")).alias("filter"),  # Assuming _id is the filter criteria
                                       col("update_doc").alias("update"))

  # Configure MongoSpark connector (replace with your connection details)
  spark.conf.set("spark.mongodb.spark.connectionString", "mongodb://your_host:port/your_database")
  spark.conf.set("spark.mongodb.output.collection", "your_collection")

  # Write data to MongoDB with replace strategy (upsert)
  try:
    upsert_data.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .option("replaceDocument", "true") \
        .save()
    print("Data upserted successfully using PySpark")
  except Exception as e:
    print("Error upserting data:", e)

# Replace with your actual list of dictionaries
data_list = [{"_id": 1, "data": {"field1": "value1"}}, {"_id": 2, "data": {"field2": "value2"}}]

upsert_to_mongo(data_list)
