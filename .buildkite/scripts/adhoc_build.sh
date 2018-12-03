#!/bin/bash

set -eo pipefail

# Script to run adhoc buildkite builds for the given branch. Useful for running
# CI tests against branches that may not yet be ready for a PR. See
# https://buildkite.com/docs/apis/rest-api#authentication for more info on
# buildkite tokens. To take full advantage of this script your token will need
# `read_builds` and `write_builds` scopes. The script expects a working
# installation of jq.

function usage() {
  echo "build.sh <build|list>"
  exit 1
}

function build() {
  if [[ -z "$BUILDKITE_CLI_TOKEN" ]]; then
    echo "BUILDKITE_CLI_TOKEN must be set"
    exit 1
  fi

  local action=$1
  local url="https://api.buildkite.com/v2/organizations/m3/pipelines/m3-monorepo-ci"
  local auth="Authorization: Bearer $BUILDKITE_CLI_TOKEN"

  if [[ "$action" != "build" && "$action" != "list" ]]; then
    usage
  fi

  if [[ "$action" == "list" ]]; then
    curl -sSf -H "$auth" "${url}/builds"
    return
  fi

  # action == build
  local branch
  branch=$(git rev-parse --abbrev-ref HEAD)

  local data
  data=$(cat <<EOF
    {
      "branch": "${branch}",
      "message": "$(whoami) adhoc build",
      "commit": "HEAD",
      "ignore_pipeline_branch_filters": true,
      "clean_checkout": true
    }
EOF
  )

  curl -sSf -X POST -H "$auth" "${url}/builds" -d "$data" | jq -r .web_url
}

build "$*"
