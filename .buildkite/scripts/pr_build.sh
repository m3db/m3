#!/bin/bash

set -exo pipefail

# Script to run adhoc buildkite builds for the given branch. Useful for running
# CI tests against branches that may not yet be ready for a PR. See
# https://buildkite.com/docs/apis/rest-api#authentication for more info on
# buildkite tokens. To take full advantage of this script your token will need
# `read_builds` and `write_builds` scopes. The script expects a working
# installation of jq.
#
# You'll also need a Github token with `public_repo` permissions.

function usage() {
  echo "build.sh \$PR_NUMBER"
  exit 1
}

if [[ -z "$GITHUB_PR_TOKEN" ]]; then
  echo "must set GITHUB_PR_TOKEN"
  exit 1
fi

if [[ -z "$BUILDKITE_CLI_TOKEN" ]]; then
  echo "must set BUILDKITE_CLI_TOKEN"
  exit 1
fi

if ! command -v jq; then
  echo "cannot find jq in PATH"
  exit 1
fi

RESP=$(mktemp)

function cleanup() {
  rm -f "$RESP"
}

trap cleanup EXIT

curl -H "Authorization: token $GITHUB_PR_TOKEN" -sSf -o "$RESP" "https://api.github.com/repos/m3db/m3/pulls/$1"

function jq_field() {
  <"$RESP" jq -r "$1"
}

function build() {
  local pr=$1
  local url="https://api.buildkite.com/v2/organizations/uberopensource/pipelines/m3-monorepo-ci"
  local auth="Authorization: Bearer $BUILDKITE_CLI_TOKEN"
  local repo; repo=$(jq_field '.head.repo.clone_url')
  local sha; sha=$(jq_field '.head.sha')
  local branch; branch=$(jq_field '.head.ref')
  local message; message=$(jq_field '.title')

  local data
  data=$(cat <<EOF
    {
      "branch": "$branch",
      "message": "$message",
      "commit": "$sha",
      "ignore_pipeline_branch_filters": true,
      "clean_checkout": true,
      "pull_request_base_branch": "master",
      "pull_request_id": $pr,
      "pull_request_repository": "$repo"
    }
EOF
  )

  curl -sSf -X POST -H "$auth" "${url}/builds" -d "$data" | jq -r .web_url
}

build "$*"
