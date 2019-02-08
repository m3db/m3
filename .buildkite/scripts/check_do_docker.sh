#!/bin/bash

set -exuo pipefail

# This script checks if we should do a docker build, and if so adds a step in
# the build pipeline to do so.

CURRENT_SHA=$(git rev-parse HEAD)
MASTER_SHA=$(git rev-parse origin/master)

# If this commit matches an exact tag, or HEAD is origin/master, kick off the
# docker step.
if git describe --tags --exact-match || [[ "$CURRENT_SHA" == "$MASTER_SHA" ]]; then
  buildkite-agent pipeline upload .buildkite/docker-pipeline.yml
fi
