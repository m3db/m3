# Built artifacts maintained externally at https://quay.io/repository/m3db/ci-base

# Base image required for CI.
# We seal the version of Golang to get hermetic builds.
FROM golang:1.10.4-stretch
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /go/src/github.com/m3db/m3
WORKDIR /go/src/github.com/m3db/m3
