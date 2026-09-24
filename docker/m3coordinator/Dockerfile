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

# Build m3coordinator binary
RUN cd /go/src/github.com/m3db/m3/ && \
    git submodule update --init      && \
    GOOS=$TARGETOS GOARCH=$TARGETARCH make m3coordinator

# stage 2: lightweight "release"
FROM alpine:3.20
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# Provide timezone data to allow TZ environment variable to be set
# for parsing relative times such as "9am" correctly and respect
# the TZ environment variable.
RUN apk add --no-cache tzdata

EXPOSE 7201/tcp 7203/tcp

COPY --from=builder /go/src/github.com/m3db/m3/bin/m3coordinator /bin/
COPY --from=builder /go/src/github.com/m3db/m3/src/query/config/m3coordinator-local-etcd.yml /etc/m3coordinator/m3coordinator.yml

ENTRYPOINT [ "/bin/m3coordinator" ]
CMD [ "-f", "/etc/m3coordinator/m3coordinator.yml" ]
