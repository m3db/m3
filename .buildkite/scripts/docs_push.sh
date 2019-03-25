#!/bin/ash
# shellcheck shell=dash

set -exo pipefail

if [ "$BUILDKITE_BRANCH" != "master" ]; then
  echo "nothing to do"
  exit 0
fi

mkdir -p "$HOME/.ssh"
ssh-keyscan github.com >> "$HOME/.ssh/known_hosts"
git config --local user.email "buildkite@m3db.io"
git config --local user.name "M3 Buildkite Bot"

rm -rf site

# NB(schallert): if updating this build step be sure to update the docs-build
# make target (see note there as to why we can't share code between the two).
mkdocs build -e docs/theme -t material

git checkout -t origin/docs
rm -rf m3db.io/*
cp -r site/* m3db.io/

# Trying to commit 0 changes would fail, so let's check if there's any changes
# queued up first. diff-index HEAD exits 0 if no pending changes.
if git diff-index --quiet HEAD --; then
  echo "no docs changes"
  exit 0
fi

git add m3db.io
git commit -m "Docs update $(date)"
git push
