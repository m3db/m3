// we can set cluster as a top level argument when running jsonnet
local cluster = "gcp";

// we need to import all environments bc import paths cannot be generated
local environments = {
    "gcp": (import "../environments/gcp.libsonnet"),
};

local env_vars = environments[cluster];
local globals = env_vars.globals;

// TODO: overwrite vars in globals with service layer globals instead of having
// two different sets of globals
local dtest_globals = env_vars.dtest.globals;

std.manifestYamlDoc(std.prune({
	dtest: {
		debugPort: dtest_globals.debug_port,
		bootstrapTimeout: dtest_globals.bootstrap_timeout,
		bootstrapReportInterval: "1m",
        nodePort: globals.m3dbnode_port,
        serviceID: "m3dbnode",
        dataDir: dtest_globals.data_dir,
        // loop through seeds to populate this list, but check if they're nil first
        seeds: if "seeds" in dtest_globals then [
			{
				namespace: seed.namespace,
				localShardNum: seed.localShardNum,
                retention: seed.retention,
                blockSize: seed.blockSize,
			},	
            for seed in dtest_globals.seeds
		] else [], 
        // same thing here about instances
		instances: if "instances" in dtest_globals then [
			{
				id: instance,
				rack: "rack-0",
                zone: "sjc1",
                weight: 1,
				hostname: instance,
			},
            for instance in dtest_globals.instances
		] else [],
	},
    m3em: {
		agentPort: dtest_globals.m3em_agent_port,
		// do not include TLS for now, should be generated at the same time as k8 clusters 
		heartbeatPort: dtest_globals.heartbeat_port,
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
		// loop thru dem clusters
		etcdClusters: [
            if cluster.zone == globals.kv_dc then
			{
				zone: cluster.zone,
				endpoints: cluster.endpoints
			},// this doesn't execute cleanly rn 
            for cluster in globals.etcd.clusters
		]
	},
}))
