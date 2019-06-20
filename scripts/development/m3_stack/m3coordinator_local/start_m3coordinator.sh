#!/usr/bin/env bash

set -xe

CURR_DIR="$(pwd)"
ROOT_DIR="${CURR_DIR}/../../../../"

BUILD="${BUILD:-true}"
if [[ "$BUILD" = true ]]; then
    echo "Compiling coordinator"
    bash -c "cd $ROOT_DIR && make m3coordinator"
fi

echo "Starting coordinator"
killall m3coordinator 1>/dev/null 2>/dev/null || echo "no running m3coordinator" 1>/dev/null 2>/dev/null
if [[ "$USE_WINDOW" = true ]]; then
    tmux new-window -d -n "m3coordinator_local" "${ROOT_DIR}/bin/m3coordinator -f ${CURR_DIR}/m3coordinator.yml"
else
    tmux new-session -d -s "m3coordinator_local" "${ROOT_DIR}/bin/m3coordinator -f ${CURR_DIR}/m3coordinator.yml"
fi
 