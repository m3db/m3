local lib = import 'm3dbnode.libsonnet';

local cluster = {
  HOST1_ETCD_ID: "host_name1",
  HOST1_ETCD_IP_ADDRESS: "host_ip1",
  HOST2_ETCD_ID: "host_name2",
  HOST2_ETCD_IP_ADDRESS: "host_ip2",
  HOST3_ETCD_ID: "host_name3",
  HOST3_ETCD_IP_ADDRESS: "host_ip3",
};

std.manifestYamlDoc(lib(cluster))
