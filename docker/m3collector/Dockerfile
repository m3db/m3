# stage 1: build
FROM golang:1.13-alpine3.11 AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install deps
RUN apk add --update git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3
ADD . /go/src/github.com/m3db/m3

# Build m3dbnode binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    make m3collector-linux-amd64

# stage 2: lightweight "release"
FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 7206-7207/tcp

RUN apk add --no-cache curl jq

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3collector /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/collector/config/m3collector.yml /etc/m3collector/m3collector.yml

ENTRYPOINT [ "/bin/m3collector" ]
CMD [ "-f", "/etc/m3collector/m3collector.yml" ]
