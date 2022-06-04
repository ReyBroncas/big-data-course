#!/bin/sh

if [ "$1" == "producer" ]; then
    docker-compose up producer
    exit
fi
if [ "$1" == "consumer" ]; then
    docker-compose up consumer
    exit
fi
if [ "$1" == "rest-service" ]; then
    docker-compose up rest-service
    exit
fi
