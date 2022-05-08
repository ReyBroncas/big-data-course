#!/bin/sh

function download_dataset() {
    echo '==[DOWNLOADING DATASET]'
    curl https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Books_v1_02.tsv.gz -o ./data/dataset1.tsv.gz
    gzip -d ./data/dataset1.tsv.gz
    echo '==[DONE]'
}

function stop() {
    echo '==[SHUTTING DOWN DOCKER CONTAINERS]'
    docker-compose down
    docker-compose rm -f
    echo '==[DONE]'
}

function parse() {
    echo '==[PARSING DATASET]'
    python parser.py ./data/dataset1.tsv ./data/dataset.tsv
    echo '==[DONE]'
}

function run() {
    echo '==[RUNNING DOCKER CONTAINER]'
    docker-compose build
    docker-compose up
    echo '==[DONE]'
}

if [ "$1" == "stop" ]; then
    stop
    exit
fi
if [ "$1" == "run" ]; then
    parse
    run
    exit
fi
