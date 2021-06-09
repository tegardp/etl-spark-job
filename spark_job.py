import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("FlightData") \
    .getOrCreate()

# Define Schema
fields = [
    StructField("airline_code", StringType(), True),
    StructField("airtime", IntegerType(), True),
    StructField("arrival_delay", IntegerType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("destination_airport", StringType(), True),
    StructField("distance", IntegerType(), True),
    StructField("flight_date", StringType(), True),
    StructField("flight_num", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("source_airport", StringType(), True),
]
schema = StructType(fields)

# Create Empty DataFrame with defined schema
flightData = spark.createDataFrame([], schema)

# Fill DataFrame with Flight Data
files = ["gs://academi/2021-04-27.json", "gs://academi/2021-04-28.json",
         "gs://academi/2021-04-29.json", "gs://academi/2021-04-30.json"]
for file in files:
    flightData = spark.read.json(file, schema=schema).union(flightData)

flightData.withColumn('flight_date', F.date_add(
    flightData['flight_date'], 723)).show()

# Saving the data to BigQuery
flightData.write.format('bigquery') \
    .option('temporaryGcsBucket', 'academi/temp') \
    .save('staging.flight_data')
