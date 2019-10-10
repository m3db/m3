FROM alpine:latest AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /bin
RUN mkdir -p /etc/m3comparator
ADD ./m3comparator /bin/

EXPOSE 9000/tcp

ENTRYPOINT [ "/bin/m3comparator" ]
CMD
