#!/bin/bash
# setup-kafka.sh

echo "Waiting for Kafka to be ready..."
sleep 20

echo "Creating tweet-events topic..."
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic tweet-events --partitions 1 --replication-factor 1

echo "Listing topics:"
kafka-topics --bootstrap-server kafka:9092 --list

echo "Kafka setup complete."