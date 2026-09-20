FROM golang:1.25.6-bookworm

RUN apt-get update && apt-get install -y lsof netcat-openbsd docker.io jq docker-compose
