#!/usr/bin/env bash

set -e

ASSET_DIR=${1:-src/coordinator/generated/assets/openapi}

# Copy over OpenAPI doc.
mkdir -p m3metrics.io/openapi
rsync -a --exclude=*.go "$ASSET_DIR"/* m3metrics.io/openapi
# Create .bak file and then delete it to make sed work for both GNU and Mac versions
sed -i.bak "s#spec-url='.*'#spec-url='spec.yml'#g" m3metrics.io/openapi/index.html
rm m3metrics.io/openapi/index.html.bak
