# us-flights-kafka-spark

python3 -m venv venv ; source venv/bin/activate

chmod +x reset_kafka.sh
chmod +x ./spark/spark_receiver.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.mongodb.spark:mongo-spark-connector_2.13:10.2.1 spark/spark_receiver.py 60 1

spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.mongodb.spark:mongo-spark-connector_2.13:10.2.1 \
  /opt/spark/spark_receiver.py 60 1
