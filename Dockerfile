FROM golang:1.20.4-bullseye

RUN apt-get update && apt-get install -y lsof
