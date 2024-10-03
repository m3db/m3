FROM golang:1.22-bullseye

RUN apt-get update && apt-get install -y lsof netcat-openbsd docker.io jq unzip wget

COPY --from=znly/protoc:0.2.0 /usr/bin/protoc /usr/local/bin/protoc
COPY --from=znly/protoc:0.2.0 /usr/include/google /usr/local/include/google
COPY --from=znly/protoc:0.2.0 /usr/include/protoc-gen-* /usr/local/include/protoc-gen-*

RUN go install github.com/gogo/protobuf/protoc-gen-gogofaster@latest

ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
