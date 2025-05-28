from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, to_timestamp, count, sum, when, to_date, length, split, lpad, concat_ws, lit, coalesce
from pyspark.sql.types import *
from spark_helper import hhmm_to_timestamp

# For anomalies detection
D_MINUTES = 60
# Number of airplanes mid air
N_THRESHOLD = 30

AIRPORTS_CSV_PATH = "data/airports.csv"

spark = SparkSession.builder \
    .appName("FlightETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# airports.csv schema
airports_schema = StructType([
    StructField("IATA_CODE", StringType(), True),
    StructField("AIRPORT", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
])

df_airports = spark.read \
    .option("header", "true") \
    .schema(airports_schema) \
    .csv(AIRPORTS_CSV_PATH) \
    .select(
        col("IATA_CODE").alias("airport_iata"),
        col("AIRPORT").alias("airport_name"),
        col("CITY").alias("airport_city"),
        col("STATE").alias("airport_state")
    ).cache()


# Schema for incoming data
schema = StructType([
    StructField("YEAR", StringType(), True),
    StructField("MONTH", StringType(), True),
    StructField("DAY", StringType(), True),
    StructField("DAY_OF_WEEK", StringType(), True),
    StructField("AIRLINE", StringType(), True),
    StructField("FLIGHT_NUMBER", StringType(), True),
    StructField("TAIL_NUMBER", StringType(), True),
    StructField("ORIGIN_AIRPORT", StringType(), True),
    StructField("DESTINATION_AIRPORT", StringType(), True),
    StructField("SCHEDULED_DEPARTURE", StringType(), True),
    StructField("DEPARTURE_TIME", StringType(), True),
    StructField("DEPARTURE_DELAY", StringType(), True),
    StructField("TAXI_OUT", StringType(), True),
    StructField("WHEELS_OFF", StringType(), True),
    StructField("SCHEDULED_TIME", StringType(), True),
    StructField("ELAPSED_TIME", StringType(), True),
    StructField("AIR_TIME", StringType(), True),
    StructField("DISTANCE", StringType(), True),
    StructField("WHEELS_ON", StringType(), True),
    StructField("TAXI_IN", StringType(), True),
    StructField("SCHEDULED_ARRIVAL", StringType(), True),
    StructField("ARRIVAL_TIME", StringType(), True),
    StructField("ARRIVAL_DELAY", StringType(), True),
    StructField("DIVERTED", StringType(), True),
    StructField("CANCELLED", StringType(), True),
    StructField("CANCELLATION_REASON", StringType(), True),
    StructField("AIR_SYSTEM_DELAY", StringType(), True),
    StructField("SECURITY_DELAY", StringType(), True),
    StructField("AIRLINE_DELAY", StringType(), True),
    StructField("LATE_AIRCRAFT_DELAY", StringType(), True),
    StructField("WEATHER_DELAY", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flights") \
    .option("startingOffsets", "earliest") \
    .load()

# from kafka to string
csv_df = df.selectExpr("CAST(value AS STRING) as csv")

# split csv into columns
data_parts = split(col("csv"), ",").alias("data")
parsed_df = csv_df.select(data_parts) \
                  .select([col("data")[i].alias(schema.fields[i].name) for i in range(len(schema.fields))])

# filter first verse:
header_filtered_df = parsed_df.filter(col("YEAR") != "YEAR")

def create_timestamp_str(year, month, day, time_hhmm):
    # fix time format HHmm (ex. '5' -> '0005', '930' -> '0930')
    padded_time = lpad(time_hhmm, 4, '0')
    hour = padded_time.substr(1, 2)
    minute = padded_time.substr(3, 2)
    
    safe_hour = when(hour == "24", "00").otherwise(hour)
    
    return concat_ws(" ",
        concat_ws("-", year, lpad(month, 2, "0"), lpad(day, 2, "0")),
        concat_ws(":", safe_hour, minute, lit("00")) # add seconds
    )

# filter rows without key time values
filtered_df = header_filtered_df.filter(
    (col("SCHEDULED_DEPARTURE").isNotNull()) & (col("SCHEDULED_DEPARTURE") != "") &
    (col("DEPARTURE_TIME").isNotNull()) & (col("DEPARTURE_TIME") != "")
)

transformed_df = filtered_df.withColumn(
    "DEPARTURE_DELAY",
    when(col("DEPARTURE_DELAY") == "", None).otherwise(col("DEPARTURE_DELAY")).cast(DoubleType())
).withColumn(
    "ARRIVAL_DELAY",
    when(col("ARRIVAL_DELAY") == "", None).otherwise(col("ARRIVAL_DELAY")).cast(DoubleType())
).withColumn(
    "CANCELLED", (col("CANCELLED") == "1").cast(BooleanType())
).withColumn(
    "DISTANCE",
    when(col("DISTANCE") == "", None).otherwise(col("DISTANCE")).cast(DoubleType())
).withColumn(
    "SCHEDULED_DEPARTURE_TS", to_timestamp(create_timestamp_str(col("YEAR"), col("MONTH"), col("DAY"), col("SCHEDULED_DEPARTURE")))
).withColumn(
    "DEPARTURE_TIME_TS", to_timestamp(create_timestamp_str(col("YEAR"), col("MONTH"), col("DAY"), col("DEPARTURE_TIME")))
).withColumn(
    "SCHEDULED_ARRIVAL_TS", to_timestamp(create_timestamp_str(col("YEAR"), col("MONTH"), col("DAY"), col("SCHEDULED_ARRIVAL")))
).withColumn(
    "ARRIVAL_TIME_TS", to_timestamp(create_timestamp_str(col("YEAR"), col("MONTH"), col("DAY"), col("ARRIVAL_TIME")))
)

df_with_delays = transformed_df \
    .withColumn("DEPARTURE_DELAY", (col("DEPARTURE_TIME_TS").cast("long") - col("SCHEDULED_DEPARTURE_TS").cast("long")) / 60) \
    .withColumn("ARRIVAL_DELAY", (col("ARRIVAL_TIME_TS").cast("long") - col("SCHEDULED_ARRIVAL_TS").cast("long")) / 60)


df_enriched = df_with_delays \
    .join(df_airports, df_with_delays.DESTINATION_AIRPORT == df_airports.airport_iata, "left_outer") \
    .withColumnRenamed("airport_name", "DEST_AIRPORT_NAME") \
    .withColumnRenamed("airport_city", "DEST_AIRPORT_CITY") \
    .withColumnRenamed("airport_state", "DEST_AIRPORT_STATE") \
    .drop("airport_iata") \
    .join(df_airports, df_with_delays.ORIGIN_AIRPORT == df_airports.airport_iata, "left_outer") \
    .withColumnRenamed("airport_name", "ORIGIN_AIRPORT_NAME") \
    .withColumnRenamed("airport_city", "ORIGIN_AIRPORT_CITY") \
    .withColumnRenamed("airport_state", "ORIGIN_AIRPORT_STATE") \
    .drop("airport_iata") \
# aggregation ETL
# agg_df = df_with_delays \
#     .withWatermark("SCHEDULED_DEPARTURE_TS", "10 minutes") \
#     .groupBy(
#         window(col("SCHEDULED_DEPARTURE_TS"), "5 minutes", "1 minute"),
#         col("ORIGIN_AIRPORT")
#     ) \
#     .agg(
#         count("*").alias("total_flights"),
#         sum("DEPARTURE_DELAY").alias("total_departure_delay_minutes"),
#         count(when(col("CANCELLED"), 1)).alias("cancelled_flights")
#     )

agg_df = df_enriched \
    .withColumn("FLIGHT_DAY", to_date(col("SCHEDULED_DEPARTURE_TS"))) \
    .groupBy("FLIGHT_DAY", "ORIGIN_AIRPORT_STATE") \
    .agg(
        count("*").alias("departure_count"),
        sum(when(col("DEPARTURE_DELAY") > 0, col("DEPARTURE_DELAY")).otherwise(0)).alias("total_departure_delay"),
        count(when(col("DEST_AIRPORT_STATE").isNotNull(), 1)).alias("arrival_count"),
        sum(when(col("ARRIVAL_DELAY") > 0, col("ARRIVAL_DELAY")).otherwise(0)).alias("total_arrival_delay")
    )


query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

#query.awaitTermination()
spark.streams.awaitAnyTermination()