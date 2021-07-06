# Compile stage
FROM golang:1.13.15 AS build-env

ENV GO111MODULE=on
ENV GOPROXY="https://proxy.golang.org,direct"

# Build Delve
#RUN CGO_ENABLED=0 go get -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@1.5.1
RUN CGO_ENABLED=0 go get -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv
#RUN go get github.com/go-delve/delve/cmd/dlv

FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 7201/tcp 7203/tcp

COPY --from=build-env /go/bin/dlv /

ADD ./m3coordinator /bin/m3coordinator
ADD ./config/m3coordinator-local-etcd.yml /etc/m3coordinator/m3coordinator.yml

#ENTRYPOINT [ "/bin/m3coordinator" ]
#CMD [ "-f", "/etc/m3coordinator/m3coordinator.yml" ]
ENTRYPOINT [ "/dlv" ]
CMD [ "--listen=:2345", "--check-go-version=false", "--headless=true", "--log=true", "--log-output=debugger,debuglineerr,gdbwire,lldbout,rpc",  "--accept-multiclient", "--api-version=2", "exec", "./bin/m3coordinator", "--", "-f", "/etc/m3coordinator/m3coordinator.yml"  ]
