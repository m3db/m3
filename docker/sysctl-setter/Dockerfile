FROM alpine:3.12
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"

RUN apk add --no-cache procps && echo $'#!/bin/ash\n\
set -e\n\
while true; do\n\
  MVAL=$(sysctl -n vm.max_map_count)\n\
  MIN_MVAL=${SYSCTL_VM_MAX_MAP_COUNT:-3000000}\n\
  if [ "$MVAL" -lt "$MIN_MVAL" ]; then\n\
    sysctl -w vm.max_map_count=${MIN_MVAL}\n\
  fi\n\
  SVAL=$(sysctl -n vm.swappiness)\n\
  if [ "$SVAL" -ne 1 ]; then\n\
    sysctl -w vm.swappiness=1\n\
  fi\n\
  FVAL=$(sysctl -n fs.file-max)\n\
  MIN_FVAL=${SYSCTL_FS_FILE_MAX:-3000000}\n\
  if [ "$FVAL" -lt "$MIN_FVAL" ]; then\n\
    sysctl -w fs.file-max=${MIN_FVAL}\n\
  fi\n\
  OVAL=$(sysctl -n fs.nr_open)\n\
  MIN_OVAL=${SYSCTL_FS_NR_OPEN:-3000000}\n\
  if [ "$OVAL" -lt "$MIN_OVAL" ]; then\n\
    sysctl -w fs.nr_open=${MIN_OVAL}\n\
  fi\n\
  sleep 60\n\
done' > /bin/m3dbnode_sysctl.sh &&\
  chmod +x /bin/m3dbnode_sysctl.sh

ENTRYPOINT [ "/bin/m3dbnode_sysctl.sh" ]
