// we can set cluster as a top level argument when running jsonnet
local cluster = std.extVar("cluster");

// we need to import all environments bc import paths cannot be generated
local environments = {
    "gcp": (import "../environments/gcp.libsonnet"),
};

// collect global variables
local env_vars = environments[cluster];
local globals = env_vars.globals + env_vars.dtest.globals;

std.prune({
	dtest: {
		debugPort: globals.debug_port,
		bootstrapTimeout: globals.bootstrap_timeout,
		bootstrapReportInterval: "1m",
        nodePort: globals.m3dbnode_port,
        serviceID: "m3dbnode",
        dataDir: globals.data_dir,
        seeds: if "seeds" in globals then [
			{
				namespace: seed.namespace,
				localShardNum: seed.localShardNum,
                retention: seed.retention,
                blockSize: seed.blockSize,
			},	
            for seed in globals.seeds
		] else [], 
		instances: if "instances" in globals then [
			{
				id: instance,
				rack: "rack-0",
                zone: "prod",
                weight: 1,
				hostname: instance,
			},
            for instance in globals.instances
		] else [],
	},
    m3em: {
		agentPort: globals.m3em_agent_port,
		// do not include TLS for now, should be generated at the same time as k8 clusters 
		heartbeatPort: globals.heartbeat_port,
        node: {
			heartbeat: {
				timeout: "4m",
		 		interval: "3s",	
			},
		},
        cluster: {
			replication: 3,
			numShards: 1024,
		    nodeConcurrency: 8,
			nodeOperationTimeout: "5m",	
		},
	},
	kv: {
		env: globals.kv_env,
		zone: globals.kv_dc,
		service: "dtest",
		cacheDir: "/var/lib/m3kv",
		etcdClusters: [
            if cluster.zone == globals.kv_dc then
			{
				zone: cluster.zone,
				endpoints: cluster.endpoints
			},
            for cluster in globals.etcd.clusters
		]
	},
})
