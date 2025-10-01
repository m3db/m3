#!/usr/bin/env bash

set -e

echo "Building bootstrap tool..."
go build -o bootstrap/m3bootstrap ./bootstrap

echo "Running bootstrap..."
./bootstrap/m3bootstrap -config "${1:-m3msg-bootstrap.yaml}"

echo "Bootstrap complete!"
