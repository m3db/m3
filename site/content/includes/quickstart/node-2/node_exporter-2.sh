#!/bin/bash
./node_exporter --collector.textfile.directory=/node-2/ --web.listen-address 127.0.0.1:8082
