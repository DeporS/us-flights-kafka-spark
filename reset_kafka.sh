#!/bin/bash

KAFKA_CONTAINER=us-flights-kafka-spark-kafka-1
BROKER=localhost:9092

# delete topic
docker exec -it $KAFKA_CONTAINER kafka-topics --delete --topic flights --bootstrap-server $BROKER || true

# create topic
docker exec -it $KAFKA_CONTAINER kafka-topics --create --topic flights --bootstrap-server $BROKER --partitions 1 --replication-factor 1

echo "Kafka topics deleted and created again."
