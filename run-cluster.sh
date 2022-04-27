#!/bin/sh

function run() {
    echo '==[RUNNING DOCKER CONTAINER]'

    docker-compose up -d

    echo '==[DONE]'
}

run
