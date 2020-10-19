#!/bin/bash

set -eo pipefail

BIN="$1"

if [[ ! -x "$BIN" ]]; then
  echo "$BIN is not a binary"
  exit 1
fi

TAGS=()
if [[ -n "$GO_BUILD_TAGS" ]]; then
  TAGS=("--build-tags" "${GO_BUILD_TAGS}")
fi

"$BIN" run "${TAGS[@]}"

# Custom lint checks
function check_correct_grpc_dial() {
  # If this needs to be removed because it doesn't make sense, then that
  # is fine and this can be disabled - but would prefer the safety for now
  # and only remove it if there really are workarounds needed for a given place.
  # It would also be preferential to just exclude the callsite that needs
  # different behavior, and/or plumb through extra options to xgrpc.Dial
  # to deal with whatever behavior required to change for that specific 
  # callsite (latter is probably preferential).
  # NB(r): In the future could make this a Go custom linter.
  echo "All call sites should use xgrpc.Dial not grpc.Dial, otherwise"
  echo "they would miss required dial options some of which are required"
  echo "for safe GRPC calls in production."
  if grep --include=*.go --exclude xgrpc.go --exclude dial.go --exclude *.pb* --exclude-dir vendor -r "[^x]grpc.Dial(" ./; then
    echo "Failing lint, found direct grpc.Dial calls"
    return 1
  fi
  if grep --include=*.go --exclude xgrpc.go --exclude *.pb* --exclude-dir vendor -r "[^x]grpc.NewServer(" ./; then
    echo "Failing lint, found direct grpc.NewServer calls"
    return 1
  fi
  return 0
}

# Run custom lint checks
check_correct_grpc_dial || exit 1
