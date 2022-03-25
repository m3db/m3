FROM alpine:3.15
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 5000/tcp 6000/tcp 6001/tcp

ADD ./m3aggregator /bin/m3aggregator
ADD ./config/m3aggregator.yml /etc/m3aggregator/m3aggregator.yml

ENTRYPOINT [ "/bin/m3aggregator" ]
CMD [ "-f", "/etc/m3aggregator/m3aggregator.yml" ]
