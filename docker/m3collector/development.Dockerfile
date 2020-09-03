FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 7206/tcp 7207/tcp

ADD ./m3collector /bin/m3collector
ADD ./config/m3collector.yml /etc/m3collector/m3collector.yml

ENTRYPOINT [ "/bin/m3collector" ]
CMD [ "-f", "/etc/m3collector/m3collector.yml" ]
