#!/bin/bash
./node_exporter --collector.textfile.directory=/node-3/ --web.listen-address 127.0.0.1:8083
