FROM alpine:latest AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /bin
RUN mkdir -p /etc/comparator
ADD ./m3comparator /bin/
ADD ./m3query /bin/
ADD ./docker-run.sh /bin/run.sh
ADD ./m3query-dev-remote.yml /etc/comparator/comparator.yml

EXPOSE 7201/tcp 7203/tcp

CMD ./bin/run.sh /etc/comparator/comparator.yml
