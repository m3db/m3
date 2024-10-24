FROM quay.io/m3db/thrift-gen:latest as thriftgen

FROM znly/protoc:0.2.0 as protoc

FROM golang:1.22-bullseye

RUN apt-get update && apt-get install -y lsof netcat-openbsd docker.io jq protobuf-compiler thrift-compiler

RUN go install github.com/gogo/protobuf/protoc-gen-gogofaster@latest

COPY --from=thriftgen /bin/thrift-gen /bin/thrift-gen
COPY --from=protoc /go/bin/protoc /go/bin/protoc

ENV GOBIN=/go/bin
ENV PATH=$PATH:/go/bin
