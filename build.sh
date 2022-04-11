#!/bin/sh

echo '==[BUILDING DOCKER IMAGE]'

docker build -t greeting .

echo '==[DONE]'

