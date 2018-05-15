FROM golang:1.10-alpine

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

ENTRYPOINT ["/go/src/github.com/m3db/m3db/bin/m3dbnode -f /go/src/github.com/m3db/m3db/config/m3dbnode.yaml"]
