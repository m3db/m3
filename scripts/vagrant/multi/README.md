# Vagrant

This allows comparisons between a feature branch (specified by a `FEATURE_DOCKER_IMAGE` environment variable), and `latest`.

It includes:
- kubernetes (using kind)
- etcd (single node)
- m3db operator
- m3db node (multi node)
- m3coordinator dedicated (two instances)
- prometheus
- grafana (accessible localhost:3333, login admin:admin)

# Requirements
Setup vagrant azure provider via [docs](https://github.com/Azure/vagrant-azure).
Or alternatively set up google provider via [docs](https://github.com/mitchellh/vagrant-google).

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
FEATURE_DOCKER_IMAGE=quay.io/m3dbtest/m3dbnode:feature PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./start_vagrant.sh
```

Stop:
```bash
PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/stop_vagrant.sh
```

Reopen tunnels (must provide $MACHINE):
```bash
MACHINE=primary PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/tunnel_vagrant.sh
```

SSH (must provide $MACHINE):
```bash
MACHINE=primary PROVIDER="google" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/ssh_vagrant.sh
```

# AZURE setup

After authorizing with azure, use preferred key as the SSH key.

Start:
```bash
FEATURE_DOCKER_IMAGE=quay.io/m3dbtest/m3dbnode:feature PROVIDER="azure" AZURE_TENANT_ID="tenant-id" AZURE_CLIENT_ID="client-id" AZURE_CLIENT_SECRET="client-secret" AZURE_SUBSCRIPTION_ID="subscription-id" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ./start_vagrant.sh
```

Stop:
```bash
PROVIDER="azure" AZURE_TENANT_ID="tenant-id" AZURE_CLIENT_ID="client-id" AZURE_CLIENT_SECRET="client-secret" AZURE_SUBSCRIPTION_ID="subscription-id" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/stop_vagrant.sh
```

Reopen tunnels (must provide $MACHINE):
```bash
MACHINE=primary PROVIDER="azure" AZURE_TENANT_ID="tenant-id" AZURE_CLIENT_ID="client-id" AZURE_CLIENT_SECRET="client-secret" AZURE_SUBSCRIPTION_ID="subscription-id" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/tunnel_vagrant.sh
```

SSH (must provide $MACHINE):
```bash
MACHINE=primary PROVIDER="azure" AZURE_TENANT_ID="tenant-id" AZURE_CLIENT_ID="client-id" AZURE_CLIENT_SECRET="client-secret" AZURE_SUBSCRIPTION_ID="subscription-id" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" ../shared/ssh_vagrant.sh
```

# Running

Once setup you can SSH into the benchmarker vm and turn on write load (scaling to a single replica is roughly equivalent to applying 10,000 writes/sec):
```bash
kubectl scale --replicas=1 deployment/promremotebench
```

## Accessing Grafana

You can access grafana by visiting `http://localhost:3333` and using username `admin` and password `admin`.
