# stage 1: build
# Build on the host platform and cross-compile for the target platform so
# multi-arch builds (docker buildx --platform) do not run Go under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.26.0-alpine3.22 AS builder
ARG TARGETOS
ARG TARGETARCH
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Install deps
RUN apk add --update git make bash

# Add source code
RUN mkdir -p /go/src/github.com/m3db/m3
ADD . /go/src/github.com/m3db/m3

# Build m3dbnode binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH make m3aggregator

# stage 2: lightweight "release"
FROM alpine:3.20
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 5000/tcp 6000/tcp 6001/tcp

RUN apk add --no-cache curl jq

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3aggregator /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/aggregator/config/m3aggregator.yml /etc/m3aggregator/m3aggregator.yml

ENTRYPOINT [ "/bin/m3aggregator" ]
CMD [ "-f", "/etc/m3aggregator/m3aggregator.yml" ]
