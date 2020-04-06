# Vagrant

This runs a Kubernetes environment on a single box, using vagrant to provision a VM either locally, or in a cloud environment.

It includes:
- kubernetes (using kind)
- etcd (single node)
- m3db operator
- m3db node (single node)
- m3coordinator dedicated (two instances)
- prometheus
- grafana (accessible localhost:3333, login admin:admin)

This is useful for benchmarking and similar needs.

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

After authorizing with gcloud, use ~/.ssh/google_compute_engine as the SSH key.

Start:
```bash
PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./start_vagrant.sh
```

Stop:
```bash
PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/stop_vagrant.sh
```

Reopen tunnels:
```bash
PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/tunnel_vagrant.sh
```

SSH:
```bash
PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/ssh_vagrant.sh
```

# Running

Once setup you can SSH in and turn on write load (scaling to a single replica is roughly equivalent to applying 10,000 writes/sec):
```bash
kubectl scale --replicas=1 deployment/promremotebench
```

## Accessing Grafana

You can access grafana by visiting `http://localhost:3333` and using username `admin` and password `admin`.
