#!/bin/bash
./node_exporter --collector.textfile.directory=/node-1/ --web.listen-address 127.0.0.1:8081
