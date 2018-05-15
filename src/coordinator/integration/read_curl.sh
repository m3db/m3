#!/bin/bash
curl -i localhost:7201/api/v1/prom/remote/read -X POST  --data-binary "@testdata/read_data.snappy"
