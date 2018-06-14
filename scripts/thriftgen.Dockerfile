# stage 1: build thrift-gen binary
FROM golang:1.10-alpine AS thriftgen
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

# install git
RUN apk add --update git glide

# get thrift-gen deps
RUN go get -u github.com/uber/tchannel-go

# build thrift-gen
RUN cd /go/src/github.com/uber/tchannel-go && \
  git checkout thrift-v1.0.0-dev &&           \
  glide install &&                            \
  go install github.com/uber/tchannel-go/thrift/thrift-gen

# stage 2: merge thrift-gen into thrift base image
FROM thrift:0.10.0
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

COPY --from=thriftgen /go/bin/thrift-gen /bin/thrift-gen

ENTRYPOINT [ "/bin/thrift-gen" ]
