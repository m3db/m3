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

# Also build & push the operator's docs.
git clean -dffx
git checkout -t origin/operator
git pull

git clone git@github.com:m3db/m3db-operator.git

(
  cd m3db-operator
  mkdocs build -t material
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
