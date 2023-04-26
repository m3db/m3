FROM alpine:3.11
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

ENV GODEBUG madvdontneed=1
EXPOSE 9000-9004/tcp

RUN apk add --no-cache tzdata curl jq libcap && \
  mkdir -p /var/lib/m3kv/ && \
  curl -L https://storage.googleapis.com/etcd/v3.5.4/etcd-v3.5.4-linux-amd64.tar.gz -o /tmp/etcd-v3.5.4-linux-amd64.tar.gz && \
  tar xzvf /tmp/etcd-v3.5.4-linux-amd64.tar.gz -C /tmp && \
  cp /tmp/etcd-v3.5.4-linux-amd64/etcdctl /bin/etcdctl && \
  rm -rf /tmp/*


# Add m3dbnode binary
ADD ./m3dbnode /bin/m3dbnode

# Use setcap to set +e "effective" and +p "permitted" to adjust the SYS_RESOURCE
# so the process can raise the hard file limit with setrlimit.
# Also provide timezone data to allow TZ environment variable to be set
# for parsing relative times such as "9am" correctly and respect
# the TZ environment variable.
RUN setcap cap_sys_resource=+ep /bin/m3dbnode

ENTRYPOINT [ "/bin/m3dbnode" ]

CMD [ "-f", "/etc/m3dbnode/m3dbnode.yml" ]
