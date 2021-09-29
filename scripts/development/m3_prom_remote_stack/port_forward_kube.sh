#!/usr/bin/env bash

set -xe

droidcli auth assume-role -r cluster-admin

kubectl proxy --accept-hosts='^localhost$,^127\.0\.0\.1$,^\[::1\],^host\.docker\.internal$'