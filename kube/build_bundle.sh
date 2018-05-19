#!/bin/bash

# Script to bundle the various YAMLs into a single file that can be used with
# `kubectl apply`. This is somewhat hacky and will be replaced with either a
# ksonnet-based build process, operator, or helm chart later on.

TARGET="bundle.yaml"

function sep() {
    echo "---" >> "$TARGET"
}

set -exuo pipefail

rm -f "$TARGET"
echo "# AUTOMATICALLY GENERATED (build_bundle.sh) - DO NOT EDIT" >> "$TARGET"
sep
# Ordering is important here (namespace must come first)
for F in m3dbnode-namespace.yaml etcd.yaml m3dbnode-configmap.yaml m3dbnode-statefulset.yaml; do
    [[ -f "$F" ]] || exit 1
    cat $F >> "$TARGET"
    sep
done
