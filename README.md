# us-flights-kafka-spark

python3 -m venv venv ; source venv/bin/activate

chmod +x reset_kafka.sh

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 spark/spark_receiver.py
