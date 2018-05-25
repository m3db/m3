local m3dbnode_retention = "12h";
local m3em_agent_port = 8370;
local m3em_agent_debug_port = 18370;

{
    globals: {
		metrics_sampling_rate: 1.0,
		metrics_sanitization: "prometheus",
		metrics_handler_path: "/metrics",
		env: "default_env",
	    zone: "embedded",	
		etcd: {
			clusters: [
				{
					zone: "embedded",
					endpoints: [
						"0.0.0.0:2379",
					],	
				},
			],
		},
		m3dbnode_port: 6155,
		m3dbnode_retention: m3dbnode_retention,
		m3dbnode_retention_test: '8h',
		m3dbnode_filepathprefix: "/var/lib/m3db",
		m3dbnode_gcpercentage: 100,
		m3dbnode_pooltype: "simple",
		m3db_shim_enabled: true,
	    m3em_agent_port: m3em_agent_port,
		m3em_agent_debug_port: m3em_agent_debug_port,
    },
	dtest: {
		globals: {
			m3em_agent_port: m3em_agent_port,
			heartbeat_port: 8384,
			debug_port: 18384,
			bootstrap_timeout: '30m',
			data_dir: "m3db-data/data",
			seeds: [
				{
					namespace: "metrics",
					localShardNum: 777,
					retention: m3dbnode_retention,
					blockSize: '2h',					
				},
			],
			instances: [
				"integration05-prod",
				"integration06-prod",
				"integration07-prod",
				"integration08-prod",
				"integration09-prod",
				"integration10-prod",
				"integration11-prod",
				"integration12-prod",
				"integration13-prod",
				"integration14-prod",
				"integration15-prod",
				"integration16-prod",
			],
		},
	},
    m3em_agent: {
        globals: {
            port: m3em_agent_port,
            debug_port: m3em_agent_debug_port,
            working_dir: "/var/m3em-agent",
            env_vars: [
                {
                    key: "UBER_DATACENTER",
                    value: "prod",
                },
            ],
        },
    },
	m3dbnode: {
		globals: {
			port: 9000,
			connections_per_host: 30,
			coordinator_port: 7201,
		},
	},
}
