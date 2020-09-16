# stage 1: build
FROM golang:1.13-alpine3.11 AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install deps
RUN apk add --update git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3
ADD . /go/src/github.com/m3db/m3

# Build m3nsch binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    make m3nsch_server-linux-amd64 && \
    make m3nsch_client-linux-amd64

# stage 2: lightweight "release"
FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3nsch_server /bin/
COPY --from=builder /go/src/github.com/m3db/m3/bin/m3nsch_client /bin/

ENTRYPOINT [ "/bin/m3nsch_server" ]
