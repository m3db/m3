# stage 1: build
FROM golang:1.16-alpine3.11 AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install deps
RUN apk add --update git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3
ADD . /go/src/github.com/m3db/m3

# Build m3coordinator binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    make m3query-linux-amd64

# stage 2: lightweight "release"
FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 7201/tcp 7203/tcp

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3query /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/query/config/m3query-local-etcd.yml /etc/m3query/m3query.yml

ENTRYPOINT [ "/bin/m3query" ]
CMD [ "-f", "/etc/m3query/m3query.yml" ]
