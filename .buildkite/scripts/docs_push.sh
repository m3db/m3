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
# NB(schallert): if updating this build step or the one below be sure to update
# the docs-build make target (see note there as to why we can't share code
# between the two).
mkdocs build -e docs/theme -t material
mkdocs gh-deploy --force --dirty

# We do two builds to ensure any behavior of gh-deploy doesn't impact the second
# build.
rm -rf site
mkdocs build -e docs/theme -t material

git checkout -t origin/docs
# Trying to commit 0 changes would fail, so let's check if there's any changes
# between docs branch and our changes.
if diff -qr site m3db.io; then
  echo "no docs changes"
else
  rm -rf m3db.io/*
  cp -r site/* m3db.io/

  git add m3db.io
  git commit -m "Docs update $(date)"
  git push
fi

# Also build & push the operator's docs.
git clean -dffx
git checkout -t origin/operator
git pull

git clone git@github.com:m3db/m3db-operator.git

(
  cd m3db-operator
  mkdocs build -e docs/theme -t material
)

if diff -qr m3db-operator/site m3db.io; then
  echo "no operator docs changes"
  exit 0
fi

rm -rf m3db.io/*
cp -r m3db-operator/site/* m3db.io
git clean -dffx -e m3db.io
git add m3db.io
git commit -m "Operator docs update $(date)"
git push
