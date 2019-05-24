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

EXPOSE 2379/tcp 2380/tcp 7201/tcp 7203/tcp 9000-9004/tcp

RUN apk add --no-cache curl jq

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3dbnode /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/dbnode/config/m3dbnode-local-etcd.yml /etc/m3dbnode/m3dbnode.yml
COPY --from=builder /go/src/github.com/m3db/m3/scripts/m3dbnode_bootstrapped.sh /bin/

# Use setcap and run as specific user
RUN apk add libcap && \
    mkdir -p /home/m3dbnode-user && \
    addgroup -S m3dbnode-group && \
    adduser -u 1000 -S -h /home/m3dbnode-user -G m3dbnode-group m3dbnode-user && \
    chown m3dbnode-user:m3dbnode-group /bin/m3dbnode && \
    setcap cap_ipc_lock=+ep /bin/m3dbnode && \
    setcap cap_sys_resource=+ep /bin/m3dbnode && \
    mkdir -p /var/lib && \
    chown -R m3dbnode-user:m3dbnode-group /var/lib

USER m3dbnode-user

ENTRYPOINT [ "/bin/m3dbnode" ]
CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
