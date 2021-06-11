import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField

spark = SparkSession \
    .builder \
    .appName("FlightData") \
    .getOrCreate()

df = spark.read \
  .format("bigquery") \
  .load("academi-315713.staging.flight_data")

df.write \
  .partitionBy("flight_date") \
  .format("com.databricks.spark.csv") \
  .option("header", "true") \
  .save("gs://academi/output/")
