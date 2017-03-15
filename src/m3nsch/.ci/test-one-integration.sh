#!/bin/bash
. "$(dirname $0)/variables.sh"

set -e

TAGS="integration"
DIR="integration"

go test -test.tags=${TAGS} -test.v -test.run $1 ./${DIR}
