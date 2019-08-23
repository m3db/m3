local lib = import 'm3dbnode.libsonnet';

local Cluster = {
  HOST1_ETCD_ID: "host_1",
  HOST1_ETCD_IP_ADDRESS: "ip1",
  HOST2_ETCD_ID: "host_2",
  HOST2_ETCD_IP_ADDRESS: "ip2",
  HOST3_ETCD_ID: "host_3",
  HOST3_ETCD_IP_ADDRESS: "ip3",
};

std.manifestYamlDoc(lib(Cluster))
