# Vagrant

This allows you to run a Kubernetes environment on multiple boxes using vagrant to provision VMs either locally or in a cloud environment.
Let's us compare a feature branch to release by specifying a `FEATURE_DOCKER_IMAGE` env variable.

It includes:
- kubernetes (using kind)
- etcd (single node)
- m3db operator
- m3db node (single node)
- m3coordinator dedicated (two instances)
- prometheus
- grafana (accessible localhost:3333, login admin:admin)

This is useful for benchmarking and similar needs.

# Requirements
Setup vagrant azure provider via [docs](https://github.com/Azure/vagrant-azure).
Or altertnatively set up google provider via [docs](https://github.com/mitchellh/vagrant-google).

# Local setup

Start:
```bash
./start_vagrant.sh
```

Stop:
```bash
./stop_vagrant.sh
```

Reopen tunnels:
```bash
./tunnel_vagrant.sh
```

SSH:
```bash
./ssh_vagrant.sh
```

# GCP setup

If you authorized with `gcloud` you can use `~/.ssh/google_compute_engine` as your SSH key.

Start:
```bash
FEATURE_DOCKER_IMAGE=quay.io/m3dbtest/m3dbnode:feature PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./start_vagrant.sh
```

Stop:
```bash
PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./stop_vagrant.sh
```

Reopen tunnels (must provide $MACHINE):
```bash
MACHINE=primary PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./tunnel_vagrant.sh
```

SSH (must provide $MACHINE):
```bash
MACHINE=primary PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./ssh_vagrant.sh
```

# Running

Once setup you can SSH into the benchmarker vm and turn on write load (scaling to a single replica is roughly equivalent to applying 10,000 writes/sec):
```bash
kubectl scale --replicas=1 deployment/promremotebench
```

## Accessing Grafana

You can access grafana by visiting `http://localhost:3333` and using username `admin` and password `admin`.
