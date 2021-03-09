FROM alpine:latest AS builder
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN mkdir -p /bin
RUN mkdir -p /etc/m3aggregator
ADD ./m3aggregator /bin/
ADD ./m3aggregator.yml /etc/m3aggregator/m3aggregator.yml

EXPOSE 6000-6001/tcp

ENV PANIC_ON_INVARIANT_VIOLATED=true

ENTRYPOINT [ "/bin/m3aggregator" ]
CMD [ "-f", "/etc/m3aggregator/m3aggregator.yml" ]
