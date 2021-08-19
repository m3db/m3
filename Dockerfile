FROM golang:1.16-stretch

RUN apt-get update && apt-get install -y lsof
