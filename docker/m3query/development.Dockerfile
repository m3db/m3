FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

EXPOSE 7201/tcp 7203/tcp

ADD ./m3query /bin/m3query
ADD ./config/m3query-local-etcd.yml /etc/m3query/m3query.yml

ENTRYPOINT [ "/bin/m3query" ]
CMD [ "-f", "/etc/m3query/m3query.yml" ]
