FROM golang:1.18-stretch

WORKDIR /app

RUN go install github.com/go-delve/delve/cmd/dlv@latest

COPY bin/m3dbnode /app/m3dbnode
COPY m3db-config-local.yaml /app/m3db-config-local.yaml
COPY run_m3dbnode_local.sh /app/run_m3dbnode_local.sh

CMD ["/app/run_m3dbnode_local.sh"]
