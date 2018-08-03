#!/usr/bin/env bash

set -e

ASSET_DIR=${1:-src/query/generated/assets/openapi}

# Copy over OpenAPI doc.
mkdir -p m3db.io/openapi
rsync -a --exclude=*.go "$ASSET_DIR"/* m3db.io/openapi
# Create .bak file and then delete it to make sed work for both GNU and Mac versions
sed -i.bak "s#spec-url='.*'#spec-url='spec.yml'#g" m3db.io/openapi/index.html
rm m3db.io/openapi/index.html.bak
