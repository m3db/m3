FROM golang:1.18-bullseye

RUN apt-get update && apt-get install -y lsof
