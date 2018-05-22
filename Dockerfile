# stage 1: build
FROM golang:1.10-alpine AS builder

# Install Glide
RUN apk add --update glide git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3db
ADD . /go/src/github.com/m3db/m3db

# Build m3dbnode binary
RUN cd /go/src/github.com/m3db/m3db/ && \
    git submodule update --init && \
    glide install && \
    make m3dbnode-linux-amd64

# stage 2: lightweight "release"
FROM alpine:latest

EXPOSE 2379/tcp 2380/tcp 7201/tcp 9000-9004/tcp

WORKDIR /m3db
COPY --from=builder /go/src/github.com/m3db/m3db/bin/m3dbnode /go/src/github.com/m3db/m3db/src/dbnode/config/*.yml /m3db/

ENTRYPOINT [ "/m3db/m3dbnode" ]
CMD [ "-f", "/m3db/m3dbnode-local-etcd.yml" ]
