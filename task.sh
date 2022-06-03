#!/bin/sh

run_command() {
    docker exec "$1" /bin/sh -c "$2"
}

if [ "$1" == "producer" ]; then
    docker-compose up producer
    exit
fi
if [ "$1" == "consumer" ]; then
    docker-compose up -d kafka-consumer
    run_command kafka-consumer "kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic tweets"
    exit
fi
