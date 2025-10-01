#!/usr/bin/env bash

set -xe

echo "Starting M3 Consumer Service..."

echo "Building m3consumer linux binary"
mkdir -p ./bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/m3consumer ./main.go

docker-compose build

docker-compose up -d

echo "Building and running bootstrap tool locally..."
go build -o bootstrap/m3bootstrap ./bootstrap

echo "Seeding m3msg topics and placements from m3msg-bootstrap.yaml"
./bootstrap/m3bootstrap -config m3msg-bootstrap.yaml

echo "Consumer service started!"
echo "- Consumer endpoint: localhost:9000"
echo "- View logs: docker-compose logs -f m3consumer01"
echo "- Stop: docker-compose down"
