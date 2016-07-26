#!/bin/bash
. "$(dirname $0)/variables.sh"

sed -i'' -e "s|$PACKAGE/||g" "$1"
