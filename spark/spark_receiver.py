from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FlightETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for incoming data
schema = StructType([
    StructField("airline", StringType()),
    StructField("flightNumber", StringType()),
    StructField("tailNumber", StringType()),
    StructField("startAirport", StringType()),
    StructField("destAirport", StringType()),
    StructField("scheduledDepartureTime", StringType()),
    StructField("scheduledDepartureDayOfWeek", IntegerType()),
    StructField("scheduledFlightTime", DoubleType()),
    StructField("scheduledArrivalTime", StringType()),
    StructField("departureTime", StringType()),
    StructField("taxiOut", DoubleType()),
    StructField("distance", DoubleType()),
    StructField("taxiIn", DoubleType()),
    StructField("arrivalTime", StringType()),
    StructField("diverted", BooleanType()),
    StructField("cancelled", BooleanType()),
    StructField("cancellationReason", StringType()),
    StructField("airSystemDelay", DoubleType()),
    StructField("securityDelay", DoubleType()),
    StructField("airlineDelay", DoubleType()),
    StructField("lateAircraftDelay", DoubleType()),
    StructField("weatherDelay", DoubleType()),
    StructField("cancelationTime", StringType()),
    StructField("orderColumn", StringType()),
    StructField("infoType", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flights") \
    .load()

values = df.selectExpr("CAST(value AS STRING)")

query = values.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

# # Przetwarzanie jako JSON lub CSV
# flights = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")


# flights = flights.withColumn("arrivalTimeTs", to_timestamp("scheduledArrivalTime", "HHmm"))

# flights = flights.withColumn("arrivalTimeShifted", expr("arrivalTimeTs - interval 30 minutes"))

