FROM alpine:latest AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /bin
RUN mkdir -p /etc/m3coordinator
ADD ./m3coordinator /bin/
ADD ./m3coordinator-local-etcd.yml /etc/m3coordinator/m3coordinator.yml

EXPOSE 7201/tcp 7203/tcp

ENTRYPOINT [ "/bin/m3coordinator" ]
CMD [ "-f", "/etc/m3coordinator/m3coordinator.yml" ]
