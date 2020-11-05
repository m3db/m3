#!/bin/bash

set -eo pipefail

BIN="$1"
TARGET="$2"

if [[ ! -x "$BIN" ]]; then
  echo "$BIN is not a binary"
  exit 1
fi

TAGS=()
if [[ -n "$GO_BUILD_TAGS" ]]; then
  TAGS=("--build-tags" "${GO_BUILD_TAGS}")
fi

"$BIN" run "$TARGET" "${TAGS[@]}"
