FROM golang:1.10-alpine
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN apk add -v --update git make bash python curl

RUN mkdir -p /go/src/github.com/m3db/m3
WORKDIR /go/src/github.com/m3db/m3