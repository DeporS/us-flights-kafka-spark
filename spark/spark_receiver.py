from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, to_timestamp, count, sum, when, to_date, length, split, lpad, concat_ws, lit, coalesce, lower, unix_timestamp, broadcast
from pyspark.sql.types import *
from spark_helper import hhmm_to_timestamp
import sys

# For anomalies detection
D_MINUTES_DEFAULT = 60
# Number of airplanes mid air
N_THRESHOLD_DEFAULT = 30

AIRPORTS_CSV_PATH = "data/airports.csv"

spark = SparkSession.builder \
    .appName("FlightETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Configuration for Anomaly Detection
try:
    D_MINUTES = int(sys.argv[1]) if len(sys.argv) > 1 else D_MINUTES_DEFAULT
    N_THRESHOLD = int(sys.argv[2]) if len(sys.argv) > 2 else N_THRESHOLD_DEFAULT
    print(f"Using D_MINUTES = {D_MINUTES}, N_THRESHOLD = {N_THRESHOLD} for anomaly detection.")
except IndexError:
    print(f"Running with default D_MINUTES = {D_MINUTES_DEFAULT}, N_THRESHOLD = {N_THRESHOLD_DEFAULT} for anomaly detection.")
    D_MINUTES = D_MINUTES_DEFAULT
    N_THRESHOLD = N_THRESHOLD_DEFAULT
except ValueError:
    print(f"Error: D_MINUTES and N_THRESHOLD must be integers. Running with defaults.")
    D_MINUTES = D_MINUTES_DEFAULT
    N_THRESHOLD = N_THRESHOLD_DEFAULT

# airports.csv schema
airports_schema = StructType([
    StructField("Airport_ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("IATA", StringType(), True),
    StructField("ICAO", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Altitude", IntegerType(), True),
    StructField("Timezone", StringType(), True), # Representing timezone offset as string e.g. "-5"
    StructField("DST", StringType(), True),
    StructField("TimezoneName", StringType(), True), # Tz database time zone name
    StructField("Type", StringType(), True),
    StructField("State", StringType(), True)
])

df_airports = spark.read \
    .option("header", "true") \
    .schema(airports_schema) \
    .csv(AIRPORTS_CSV_PATH) \
    .select(
        col("IATA").alias("airport_iata"),
        col("Name").alias("airport_name"),
        col("City").alias("airport_city"),
        col("State").alias("airport_state"),
        col("TimezoneName").alias("airport_timezone_name") # For potential timezone conversions
    ).filter(col("airport_iata").isNotNull() & (col("airport_iata") != "")) \
    .cache()


# Schema for incoming data
flight_event_schema = StructType([
    StructField("airline", StringType(), True),
    StructField("flightNumber", StringType(), True),
    StructField("tailNumber", StringType(), True),
    StructField("startAirport", StringType(), True), # IATA code
    StructField("destAirport", StringType(), True),  # IATA code
    StructField("scheduledDepartureTime", StringType(), True), # HHMM local
    StructField("scheduledDepartureDayOfWeek", StringType(), True),
    StructField("scheduledFlightTime", StringType(), True), # in minutes
    StructField("scheduledArrivalTime", StringType(), True), # HHMM local
    StructField("departureTime", StringType(), True), # HHMM local
    StructField("taxiOut", StringType(), True), # in minutes
    StructField("distance", StringType(), True), # in miles
    StructField("taxiIn", StringType(), True), # in minutes
    StructField("arrivalTime", StringType(), True), # HHMM local
    StructField("diverted", StringType(), True), # 1 for true, 0 for false
    StructField("cancelled", StringType(), True), # 1 for true, 0 for false
    StructField("cancellationReason", StringType(), True), # A, B, C, D
    StructField("airSystemDelay", StringType(), True), # in minutes
    StructField("securityDelay", StringType(), True), # in minutes
    StructField("airlineDelay", StringType(), True), # in minutes
    StructField("lateAircraftDelay", StringType(), True), # in minutes
    StructField("weatherDelay", StringType(), True), # in minutes
    StructField("cancelationTime", StringType(), True), # HHMM local (assuming, needs clarification)
    StructField("orderColumn", StringType(), True), # Assuming this is a full timestamp string, e.g., "YYYY-MM-DD HH:MM:SS"
    StructField("infoType", StringType(), True) # D, A, C
])

# Read from Kafka
raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flights") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize Kafka message
csv_stream_df = raw_kafka_stream.selectExpr("CAST(value AS STRING) as csv")

# split csv into columns
data_parts = split(col("csv"), ",").alias("data_array")
parsed_stream_df = csv_stream_df.select(data_parts) \
    .select([col("data_array")[i].alias(flight_event_schema.fields[i].name) for i in range(len(flight_event_schema.fields))])

# filter first verse:
filtered_stream_df = parsed_stream_df.filter(lower(col("airline")) != "airline")

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


# Base event timestamp from orderColumn and derived timestamps
transformed_df = filtered_stream_df \
    .withColumn("event_timestamp",
                when(col("orderColumn").isNotNull() & (col("orderColumn") != "") & (col("orderColumn") != '""'),
                     to_timestamp(col("orderColumn"), "yyyy-MM-dd HH:mm:ss"))
                .otherwise(None)) \
    .withColumn("flight_date", to_date(col("event_timestamp")))

# Add specific event timestamps
transformed_df = transformed_df \
    .withColumn("scheduled_departure_ts",
                when(col("scheduledDepartureTime").isNotNull() & (col("scheduledDepartureTime") != "") & (col("scheduledDepartureTime") != '""'),
                     to_timestamp(col("scheduledDepartureTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
                .otherwise(None)) \
    .withColumn("departure_ts",
                when(col("departureTime").isNotNull() & (col("departureTime") != "") & (col("departureTime") != '""'),
                     to_timestamp(col("departureTime"), "yyyy-MM-dd HH:mm:ss"))
                .otherwise(None)) \
    .withColumn("scheduled_arrival_ts",
                when(col("scheduledArrivalTime").isNotNull() & (col("scheduledArrivalTime") != "") & (col("scheduledArrivalTime") != '""'),
                     to_timestamp(col("scheduledArrivalTime"), "yyyy-MM-dd HH:mm:ss"))
                .otherwise(None)) \
    .withColumn("arrival_ts",
                when(col("arrivalTime").isNotNull() & (col("arrivalTime") != "") & (col("arrivalTime") != '""'),
                     to_timestamp(col("arrivalTime"), "yyyy-MM-dd HH:mm:ss"))
                .otherwise(None))


# Add typed columns and calculate delays
typed_df = transformed_df \
    .withColumn("is_cancelled", col("cancelled") == "1") \
    .withColumn("is_diverted", col("diverted") == "1") \
    .withColumn("departure_delay_minutes",
                (unix_timestamp(col("departure_ts")) - unix_timestamp(col("scheduled_departure_ts"))) / 60) \
    .withColumn("arrival_delay_minutes",
                (unix_timestamp(col("arrival_ts")) - unix_timestamp(col("scheduled_arrival_ts"))) / 60) \
    .withColumn("scheduled_flight_time_minutes", col("scheduledFlightTime").cast(DoubleType())) \
    .withColumn("distance_miles", col("distance").cast(DoubleType()))


# Watermarking for stream processing (using orderColumn's timestamp)
watermarked_df = typed_df.withWatermark("event_timestamp", "5 minutes")


# Aggregations:
# 1. Number of departures per day and origin state
# 2. Sum of positive departure delays per day and origin state
# 3. Number of arrivals per day and destination state
# 4. Sum of positive arrival delays per day and destination state


# Departures processing
departures_for_union = watermarked_df \
    .filter(col("infoType") == "D") \
    .join(broadcast(df_airports.alias("origin_airports")), col("startAirport") == col("origin_airports.airport_iata"), "left_outer") \
    .select(
        col("flight_date"),
        col("origin_airports.airport_state").alias("state"),
        col("departure_delay_minutes").alias("delay_value"), # Ogólna nazwa dla opóźnienia
        lit("departure").alias("event_type"),
        col("event_timestamp") # Zachowaj dla potencjalnego watermark na połączonym strumieniu
    ) \
    .filter(col("state").isNotNull())

# Przygotuj dane o przylotach do unii
arrivals_for_union = watermarked_df \
    .filter(col("infoType") == "A") \
    .join(broadcast(df_airports.alias("dest_airports")), col("destAirport") == col("dest_airports.airport_iata"), "left_outer") \
    .select(
        col("flight_date"),
        col("dest_airports.airport_state").alias("state"),
        col("arrival_delay_minutes").alias("delay_value"), # Ogólna nazwa dla opóźnienia
        lit("arrival").alias("event_type"),
        col("event_timestamp") # Zachowaj dla potencjalnego watermark na połączonym strumieniu
    ) \
    .filter(col("state").isNotNull())

unioned_events_df = departures_for_union.unionByName(arrivals_for_union, allowMissingColumns=True) # allowMissingColumns=True na wszelki wypadek, choć schematy powinny pasować


final_etl_agg = unioned_events_df \
    .groupBy("flight_date", "state") \
    .agg(
        sum(when(col("event_type") == "departure", lit(1)).otherwise(lit(0))).alias("departure_count"),
        sum(when((col("event_type") == "departure") & (col("delay_value") > 0), col("delay_value")).otherwise(lit(0.0))).alias("total_positive_departure_delay"),
        sum(when(col("event_type") == "arrival", lit(1)).otherwise(lit(0))).alias("arrival_count"),
        sum(when((col("event_type") == "arrival") & (col("delay_value") > 0), col("delay_value")).otherwise(lit(0.0))).alias("total_positive_arrival_delay")
    )

etl_query_combined = final_etl_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/etl_combined_v2") \
    .queryName("ETL_Combined_Realtime_V2") \
    .start()


anomaly_input_df = watermarked_df \
    .filter(col("infoType") == "D") \
    .filter(col("is_cancelled") == False) \
    .filter(col("is_diverted") == False) \
    .filter(col("scheduled_arrival_ts").isNotNull()) \
    .select(
        col("destAirport"),
        col("scheduled_arrival_ts"),
        col("event_timestamp") # for potential windowing if needed, though anomaly is processing time based
    )


# The anomaly detection runs every 10 minutes of processing time.
# Inside each batch, it calculates the future window.
def process_anomaly_batch(batch_df, batch_id):
    print(f"--- Anomaly Detection Batch ID: {batch_id} ---")
    current_proc_time = spark.sql("SELECT current_timestamp() as now").collect()[0]['now']
    
    anomaly_window_start = current_proc_time + expr(f"INTERVAL 30 MINUTES")
    anomaly_window_end = anomaly_window_start + expr(f"INTERVAL {D_MINUTES} MINUTES")

    print(f"Anomaly Scan Window: {anomaly_window_start} to {anomaly_window_end}")

    # Filter flights whose scheduled_arrival_ts falls into the dynamic window
    # batch_df here is a micro-batch from anomaly_input_df
    anomalous_flights = batch_df \
        .filter(
            (col("scheduled_arrival_ts") >= anomaly_window_start) &
            (col("scheduled_arrival_ts") < anomaly_window_end)
        )

    # Count flights per destination airport within this window
    airport_arrival_counts = anomalous_flights \
        .groupBy("destAirport") \
        .agg(count("*").alias("planes_in_window"))

    # Total flights heading to any airport (in this batch, not just window)
    # This provides context: "liczbę wszystkich samolotów lecących do lotniska"
    # Interpretation: "all aircraft in the current stream batch data that are scheduled to arrive at this airport eventually"
    total_flights_to_airport_in_batch = batch_df \
        .groupBy("destAirport") \
        .agg(count("*").alias("total_planes_to_airport_in_batch"))

    # Detect anomalies (N_THRESHOLD)
    potential_anomalies = airport_arrival_counts \
        .filter(col("planes_in_window") >= N_THRESHOLD) \
        .join(broadcast(df_airports), col("destAirport") == col("airport_iata"), "inner") \
        .join(total_flights_to_airport_in_batch, "destAirport", "left_outer") \
        .select(
            lit(anomaly_window_start).alias("analysis_window_start"),
            lit(anomaly_window_end).alias("analysis_window_end"),
            col("airport_name"),
            col("airport_iata"),
            col("airport_city"),
            col("airport_state"),
            col("planes_in_window"),
            coalesce(col("total_planes_to_airport_in_batch"), lit(0)).alias("total_scheduled_to_airport_batch")
        )

    if not potential_anomalies.rdd.isEmpty():
        print(f"ANOMALIES DETECTED (Batch ID: {batch_id}):")
        potential_anomalies.show(truncate=False)
    else:
        print(f"No anomalies detected in this interval (Batch ID: {batch_id}).")

# Anomaly Detection Stream (using foreachBatch for dynamic window calculation)
anomaly_query = anomaly_input_df.writeStream \
    .foreachBatch(process_anomaly_batch) \
    .trigger(processingTime="10 minutes") \
    .outputMode("update") \
    .queryName("Anomaly_Detection_Realtime") \
    .start()

#query.awaitTermination()
spark.streams.awaitAnyTermination()