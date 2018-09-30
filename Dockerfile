FROM golang:1.10-alpine

RUN mkdir -p /go/src/github.com/m3db/m3
WORKDIR /go/src/github.com/m3db/m3