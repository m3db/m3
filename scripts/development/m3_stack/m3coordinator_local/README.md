# Local Coordinator Development

This directory contains configuration for a m3coordinator process to talk to a local docker M3 stack deployment using the scripts in `m3_stack`.

# Usage

First start the M3 stack in the directory above with:
```
SEED_HOST=host.docker.internal ./start_m3.sh
```

Then use the `start_coordinator.sh` and `stop_coordinator.sh` scripts.

# Send writes to coordinator

Adjust environment variables as necessary:
```
docker run --rm -it -e PROMREMOTEBENCH_TARGET=http://host.docker.internal:17201/api/v1/prom/remote/write -e PROMREMOTEBENCH_NUM_HOSTS=1000 -e PROMREMOTEBENCH_INTERVAL=10 -e PROMREMOTEBENCH_BATCH=128 quay.io/m3db/promremotebench:latest
```
