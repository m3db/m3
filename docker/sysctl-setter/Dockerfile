FROM alpine:latest
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN apk add --no-cache procps && echo $'#!/bin/ash\n\
set -e\n\
while true; do\n\
  MVAL=$(sysctl -n vm.max_map_count)\n\
  if [ "$MVAL" -lt 3000000 ]; then\n\
    sysctl -w vm.max_map_count=3000000\n\
  fi\n\
  SVAL=$(sysctl -n vm.swappiness)\n\
  if [ "$SVAL" -ne 1 ]; then\n\
    sysctl -w vm.swappiness=1\n\
  fi\n\
  FVAL=$(sysctl -n fs.file-max)\n\
  if [ "$FVAL" -lt 3000000 ]; then\n\
    sysctl -w fs.file-max=3000000\n\
  fi\n\
  OVAL=$(sysctl -n fs.nr_open)\n\
  if [ "$OVAL" -lt 3000000 ]; then\n\
    sysctl -w fs.nr_open=3000000\n\
  fi\n\
  sleep 60\n\
done' > /bin/m3dbnode_sysctl.sh &&\
  chmod +x /bin/m3dbnode_sysctl.sh

ENTRYPOINT [ "/bin/m3dbnode_sysctl.sh" ]
