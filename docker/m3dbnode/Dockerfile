# stage 1: build
FROM golang:1.10-alpine AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install Glide
RUN apk add --update glide git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3
ADD . /go/src/github.com/m3db/m3

# Build m3dbnode binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    make m3dbnode-linux-amd64

# stage 2: lightweight "release"
FROM alpine:latest
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 2379/tcp 2380/tcp 7201/tcp 9000-9004/tcp

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3dbnode /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/dbnode/config/m3dbnode-local-etcd.yml /etc/m3dbnode/m3dbnode.yml

ENTRYPOINT [ "/bin/m3dbnode" ]
CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
