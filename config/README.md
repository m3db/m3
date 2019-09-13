## WARNING: This documentation is not complete. 

# Configs

This directory contains all of the configs needed to run M3 in a variety of modes (local, clustered, etc.)

## How-To

Below are instructions for how to generate a `yaml` file for the clustered M3DB jsonnet file. Other jsonnet files follow a similar pattern.

1. Navigate to `./m3db/clustered-etcd` directory
2. Add any changes to either the `db` or `coordinator` sections of the config that you want to change from the base file (`m3dbnode.libsonnet`) in `m3dbnode_cmd.libsonnet` using local variables. For example, to change the listen address of the `coordinator` as well as input the hosts and IP addresses of the etcd nodes:

```
local lib = import 'm3dbnode.libsonnet';

local cluster = {
  HOST1_ETCD_ID: "host_name1",
  HOST1_ETCD_IP_ADDRESS: "host_ip1",
  HOST2_ETCD_ID: "host_name2",
  HOST2_ETCD_IP_ADDRESS: "host_ip2",
  HOST3_ETCD_ID: "host_name3",
  HOST3_ETCD_IP_ADDRESS: "host_ip3",
};

local coordinator = {
        "listenAddress": {
            "type": "config",
            "value": "localhost:7208"
    },
};

std.manifestYamlDoc(lib(cluster, coordinator))
```

3. Run the following `jsonnet` command. This will generate a file called `generated.yaml` within the directory.
```
	make config-gen
```