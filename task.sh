#!/bin/sh

run_command() {
    docker exec "$1" /bin/sh -c "$2"
}

if [ "$1" == "producer" ]; then
    docker-compose up -d kafka-producer
    docker exec -it kafka-producer /bin/bash -c "kafka-console-producer.sh --broker-list kafka:9092 --topic test-topic"
    exit
fi
if [ "$1" == "consumer" ]; then
    docker-compose up -d kafka-consumer
    run_command kafka-consumer "kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic"
    exit
fi

run_command kafka "kafka-topics.sh --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092"
run_command kafka "kafka-topics.sh --describe --topic test-topic --bootstrap-server kafka:9092"

# run_command kafka "kafka-topics.sh --delete --topic test-topic --bootstrap-server localhost:9092"
