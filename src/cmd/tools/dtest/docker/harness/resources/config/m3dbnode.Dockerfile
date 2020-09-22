FROM alpine:latest AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /bin
RUN mkdir -p /etc/m3dbnode
ADD ./m3dbnode /bin/
ADD ./m3dbnode.yml /etc/m3dbnode/m3dbnode.yml

EXPOSE 2379/tcp 2380/tcp 7201/tcp 7203/tcp 9000-9004/tcp

ENTRYPOINT [ "/bin/m3dbnode" ]
CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
