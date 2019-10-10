FROM alpine:latest AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /bin
RUN mkdir -p /etc/m3query
ADD ./m3query /bin/
ADD ./m3query-dev-remote.yml /etc/m3query/m3query.yml

EXPOSE 7201/tcp 7203/tcp

ENTRYPOINT [ "/bin/m3query" ]
CMD [ "-f", "/etc/m3query/m3query.yml" ]
