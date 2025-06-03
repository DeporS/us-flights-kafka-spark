#!/bin/bash
set -e

KAFKA_CONTAINER=us-flights-kafka-spark-kafka-1
BROKER=kafka:9092

# Wait for kafka to work
for i in {1..10}; do
  echo "Waiting for kafka broker ($i)..."
  nc -z kafka 9092 && break
  sleep 5
done

# delete topic
echo "Attempting to delete topic 'flights'..."
kafka-topics.sh --delete --topic flights --bootstrap-server $BROKER || echo "Topic 'flights' did not exist or couldn't be deleted. Continuing..."

sleep 3

# create topic
echo "Attempting to create topic 'flights'..."
kafka-topics.sh --create --topic flights --bootstrap-server $BROKER --partitions 1 --replication-factor 1
if [ $? -ne 0 ]; then
    echo "Failed to create topic 'flights'. Trying again in 5 seconds..."
    sleep 5
    kafka-topics.sh --create --topic flights --bootstrap-server $BROKER --partitions 1 --replication-factor 1 || \
    (echo "Failed to create topic 'flights' on second attempt. Exiting." && exit 1)
fi

echo "Kafka topic 'flights' (re)created."

echo "Starting Python producer..."
python3 kafka_producer.py