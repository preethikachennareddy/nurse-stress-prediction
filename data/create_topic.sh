#!/bin/bash
echo "Waiting for Kafka to be ready"
# Wait until Kafka broker is listening
while ! nc -z kafka 9092; do
  sleep 1
done

echo "Creating topic 'stress-topic' with 2 partitions"
kafka-topics \
  --create \
  --topic stress-topic \
  --bootstrap-server kafka:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

echo "Topic created (or already exists). Exiting."
