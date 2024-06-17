#!/bin/bash

IMAGE_URL="hezu-app"

IMAGE_VERSION="$1"

docker build --no-cache -t ${IMAGE_URL}:${IMAGE_VERSION} -t ${IMAGE_URL}:latest . \
-f ./Dockerfile

docker push ${IMAGE_URL}:${IMAGE_VERSION}
docker push ${IMAGE_URL}:latest
