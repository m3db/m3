# Vagrant

This allows you to run a Kubernetes environment on a single box using vagrant to provision a VM either local or in a cloud environment.

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
BOX="ubuntu/xenial64" vagrant up --provider=virtualbox
```

Stop:
```bash
BOX="ubuntu/xenial64" vagrant destroy
```

# GCP setup

If you authorized with `gcloud` you can use `~/.ssh/google_compute_engine` as your SSH key.

Start:
```bash
BOX="google/gce" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" vagrant up --provider google
```

Stop:
```bash
BOX="google/gce" GOOGLE_PROJECT_ID="your_google_project_id" GOOGLE_JSON_KEY_LOCATION="your_google_service_account_json_key_as_local_path" USER="$(whoami)" SSH_KEY="your_ssh_key_as_local_path" vagrant destroy
```

# Running

Once you have your vagrant box provisioned you can setup the Kubernetes cluster with:
```bash
your_env_vars... vagrant ssh
cd provision
./setup_kube.sh
```

This should setup your cluster and also turn on a write load that can be scaled up from a single replica (for 10,000 writes/sec) with:
```bash
kubectl scale --replicas=1 deployment/promremotebench
```

## Accessing Grafana

You can access grafana by visiting `http://localhost:3333` and using username `admin` and password `admin`.
