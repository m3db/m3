#!/bin/bash

set -exo pipefail

if [[ "$BUILDKITE_BRANCH" != "master" ]]; then
  echo "nothing to do"
  exit 0
fi

buildkite-agent pipeline upload .buildkite/docs-build-pipeline.yml
