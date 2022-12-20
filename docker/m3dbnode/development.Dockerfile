FROM multiarch/alpine:x86_64-v3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

ENV GODEBUG madvdontneed=1

RUN apk add --no-cache curl jq

# Add m3dbnode binary
ADD ./m3dbnode /bin/m3dbnode
ADD ./config/m3dbnode-local-etcd.yml /etc/m3dbnode/m3dbnode.yml

EXPOSE 2379/tcp 2380/tcp 7201/tcp 7203/tcp 9000-9004/tcp

ENV GODEBUG madvdontneed=1

ENTRYPOINT [ "/bin/m3dbnode" ]
CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
