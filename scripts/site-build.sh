#!/usr/bin/env bash

set -e

ASSET_DIR=${1:-src/query/generated/assets/openapi}

# Copy over OpenAPI doc.
mkdir -p site/static/openapi
rsync -a --exclude=*.go "$ASSET_DIR"/* site/static/openapi
# Create .bak file and then delete it to make sed work for both GNU and Mac versions
sed -i.bak "s#spec-url='.*'#spec-url='spec.yml'#g" site/static/openapi/index.html
rm site/static/openapi/index.html.bak

# Now run hugo
cd site
hugo