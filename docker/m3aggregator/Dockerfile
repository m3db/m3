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
    make m3aggregator-linux-amd64

# stage 2: lightweight "release"
FROM alpine:3.15
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 5000/tcp 6000/tcp 6001/tcp

RUN apk add --no-cache curl jq

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3aggregator /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/aggregator/config/m3aggregator.yml /etc/m3aggregator/m3aggregator.yml

ENTRYPOINT [ "/bin/m3aggregator" ]
CMD [ "-f", "/etc/m3aggregator/m3aggregator.yml" ]
