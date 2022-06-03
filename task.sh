#!/bin/sh

run_command() {
    docker exec "$1" /bin/sh -c "$2"
}

if [ "$1" == "producer" ]; then
    docker-compose up producer
    exit
fi
if [ "$1" == "consumer" ]; then
    docker-compose up consumer
    exit
fi
