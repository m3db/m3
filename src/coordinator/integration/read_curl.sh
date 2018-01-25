#!/bin/bash
curl -i localhost:7201/api/v1/prom/read -X POST  --data-binary "@testdata/read_data.snappy"
