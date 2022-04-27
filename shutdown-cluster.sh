#!/bin/sh

function run() {
    echo '==[SHUTTING DOWN DOCKER CONTAINERS]'

    docker-compose down
    docker-compose rm -f 

    echo '==[DONE]'
}

run
