#!/usr/bin/env bash

set -xe

kubectl proxy --accept-hosts='^localhost$,^127\.0\.0\.1$,^\[::1\],^host\.docker\.internal$'