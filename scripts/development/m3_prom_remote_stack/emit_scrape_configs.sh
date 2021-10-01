#!/usr/bin/env bash

set -xe

nodes=()
while IFS='' read -r line; do nodes+=("$line"); done < <(curl http://localhost:8001/api/v1/nodes | jq '.items[].metadata.name' | tr -d \")


cp prometheus-scraper.yml prometheus-scraper.yml.tmp

limit=10

i=0
for node in "${nodes[@]}" ; do
  i=$((i+1))
  if [ "$i" -gt "$limit" ]; then
    break;
  fi
  echo "
  - job_name: cadvisor_${node}
    metrics_path: /api/v1/nodes/${node}/proxy/metrics/cadvisor
    static_configs:
      - targets:
        - host.docker.internal:8001
        labels:
          instance: ${node}
  " >> prometheus-scraper.yml.tmp
done
