#!/bin/bash

# Script to bundle the various YAMLs into a single file that can be used with
# `kubectl apply`. This is somewhat hacky and will be replaced with either a
# ksonnet-based build process, operator, or helm chart later on.


function sep() {
    echo "---" >> "$TARGET"
}

set -euo pipefail

DIR=$(dirname "$(dirname "${BASH_SOURCE[0]}")")
TARGET="$DIR/bundle.yaml"

rm -f "$TARGET"
echo "# AUTOMATICALLY GENERATED (build_bundle.sh) - DO NOT EDIT" >> "$TARGET"
sep
# Ordering is important here (namespace must come first)
for N in m3dbnode-namespace.yaml etcd.yaml m3dbnode-configmap.yaml m3dbnode-statefulset.yaml; do
    F="$DIR/$N"
    [[ -f "$F" ]] || exit 1
    cat "$F" >> "$TARGET"
    sep
done
