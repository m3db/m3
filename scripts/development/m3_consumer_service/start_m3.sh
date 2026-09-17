#!/usr/bin/env bash

set -xe

echo "Starting M3 Consumer Service..."

echo "Building m3consumer linux binary"
mkdir -p ./bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/m3consumer ./main.go

docker-compose build

docker-compose up -d

echo "Building m3bootstrap linux binary"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/m3bootstrap ./bootstrap

echo "Seeding m3msg topics and placements from m3_consumer_service/m3msg-bootstrap.yaml"
docker-compose run --rm bootstrap

echo "Consumer service started!"
echo "- Consumer endpoint: localhost:9000"
echo "- View logs: docker-compose logs -f m3consumer01"
echo "- Stop: docker-compose down"
