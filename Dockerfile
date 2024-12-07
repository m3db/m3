FROM golang:1.22-bullseye

RUN apt-get update && apt-get install -y lsof netcat-openbsd docker.io jq protobuf-compiler thrift-compiler

RUN go install github.com/gogo/protobuf/protoc-gen-gogofaster@latest

ENV GOBIN=/go/bin
ENV PATH=$PATH:/go/bin