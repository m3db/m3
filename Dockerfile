FROM golang:1.18-stretch

RUN apt-get update && apt-get install -y lsof
