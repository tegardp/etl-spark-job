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
    StructField("flight_date", DateType(), True),
    StructField("flight_num", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("source_airport", StringType(), True),
]
schema = StructType(fields)

# Fill DataFrame with Flight Data
flightData = spark.read.json("gs://academi/data/*.json", schema=schema)

flightData.withColumn('flight_date', F.date_add(
    flightData['flight_date'], 723)).show()

# Saving the data to BigQuery
flightData.write.format('bigquery') \
    .option('temporaryGcsBucket', 'academi/temp') \
    .option('partitionField', 'flight_date') \
    .save('staging.flight_data')

# Aggregation
aggregated = flightData.groupBy('airline_code')
aggregated = aggregated.agg({'airline_code': 'count', 'arrival_delay': 'average', }).orderBy(
    'avg(arrival_delay)').show()
