FROM golang:1.26.0-bookworm

RUN apt-get update && apt-get install -y lsof netcat-openbsd docker.io jq docker-compose
