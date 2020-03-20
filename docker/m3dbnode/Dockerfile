# stage 1: build. We have to use an older version of alpine, as git 2.22 removes
# the '-x' flag to a submodule command which in turn breaks glide, the old AF
# dependency tool we have. Until we migrate to a new version of glide or go
# modules, we need to use this older base image.
# https://github.com/m3db/m3/issues/628
FROM golang:1.14-alpine3.11 AS builder
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

# Stage 2: lightweight "release"
FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

ENV GODEBUG madvdontneed=1

EXPOSE 2379/tcp 2380/tcp 7201/tcp 7203/tcp 9000-9004/tcp

RUN apk add --no-cache curl jq

COPY --from=builder /go/src/github.com/m3db/m3/src/dbnode/config/m3dbnode-local-etcd.yml /etc/m3dbnode/m3dbnode.yml
COPY --from=builder /go/src/github.com/m3db/m3/bin/m3dbnode \
  /go/src/github.com/m3db/m3/scripts/m3dbnode_bootstrapped.sh \
  /bin/

ENTRYPOINT [ "/bin/m3dbnode" ]
CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
