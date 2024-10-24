FROM quay.io/m3db/thrift-gen:latest as thrift-gen

FROM golang:1.22-bullseye

RUN apt-get update && apt-get install -y lsof netcat-openbsd docker.io jq protobuf-compiler

ENV GOBIN=/go/bin
ENV PATH=$PATH:/go/bin

RUN go install github.com/gogo/protobuf/protoc-gen-gogofaster@latest

COPY --from=thrift-gen /bin/thrift-gen /usr/local/bin/thrift-gen
