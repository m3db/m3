# stage 1: build
FROM golang:1.18-alpine3.15 AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install deps
RUN apk add --update git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3
ADD . /go/src/github.com/m3db/m3

# Build m3dbnode binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    make m3dbnode-linux-amd64

# Stage 2: lightweight "release"
FROM alpine:3.15
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

ENV GODEBUG madvdontneed=1

EXPOSE 2379/tcp 2380/tcp 7201/tcp 7203/tcp 9000-9004/tcp

# Provide timezone data to allow TZ environment variable to be set
# for parsing relative times such as "9am" correctly and respect
# the TZ environment variable.
RUN apk add --no-cache tzdata curl jq

COPY --from=builder /go/src/github.com/m3db/m3/src/dbnode/config/m3dbnode-local-etcd.yml /etc/m3dbnode/m3dbnode.yml
COPY --from=builder /go/src/github.com/m3db/m3/bin/m3dbnode \
  /go/src/github.com/m3db/m3/scripts/m3dbnode_bootstrapped.sh \
  /bin/

ENV GODEBUG madvdontneed=1

ENTRYPOINT [ "/bin/m3dbnode" ]
CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
