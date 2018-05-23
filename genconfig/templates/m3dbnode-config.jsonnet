// we can set cluster as a top level argument when running jsonnet
local cluster = std.extVar("cluster");

// we need to import all environments bc import paths cannot be generated
local environments = {
    "gcp": (import "../environments/gcp.libsonnet"),
};

local env_vars = environments[cluster];
local globals = env_vars.globals + env_vars.m3dbnode.globals;

std.prune({
    coordinator: {
        listenAddress: "0.0.0.0:" + globals.coordinator_port,
    },
    // TODO: continue combing thru changes
    db: {
        listenAddress: "0.0.0.0:" + globals.port,
        clusterListenAddress: "127.0.0.1:0",
        httpClusterListenAddress: "0.0.0.0:" + globals.httpPort,
        httpNodeListenAddress: "0.0.0.0:" + (globals.httpPort+10000),
        debugListenAddress: "0.0.0.0:" + (globals.port+10000),
        hostId: {
            resolver: "hostname",
        },
        client: {
            writeConsistencyLevel: "majority",
            readConsistencyLevel: "unstrict_majority",
            connectConsistencyLevel: "any",
            writeTimeout: "10s",
            fetchTimeout: "15s",
            connectTimeout: "20s",
            writeRetry: {
                initialBackoff: "500ms",
                backoffFactor: 3,
                maxRetris: 2,
                jitter: true,   
            },
            fetchRetry: {
                initialBackoff: "500ms",
                backoffFactor: 3,
                maxRetris: 2,
                jitter: true,   
            },
            backgroundHealthCheckFailLimit: 4,
            backgroundHealthCheckFailThrottleFactor: 0.5, 
        }, 
        gcPercentage: globals.m3dbnode_gcpercentage,
        writeNewSeriesAsync: true,
        writeNewSeriesLimitPerSecond: 1048576, // 2 ^ 20 
        writeNewSeriesBackoffDuration: "2ms",
        cache: {
            series: {
                policy: "recently_read", 
            },
        },
        bootstrap: {
            bootstrappers: [
                "filesystem",
                "peers",
                "noop-all",
            ],
            fs: {
                numProcessorsPerCPU: 0.125 // 4 cpus for 32 node cpu
            },
            peers: {
                fetchBlocksMetadataEndpointVersion: "v2",
            },
        },
        commitlog: {
            flushMaxBytes: 524288, // 2^19
            flushEvery: "1s",
            queue: {
                calculationType: "fixed",
                size: 2097152, // 2 ^ 21
            },
            retentionPeriod: "24h",
            blockSize: if "m3dbnode_commitlog_blocksize" in globals then globals.m3dbnode_commitlog_blocksize else "10m",
        },
        fs: {
            filePathPrefix: globals.m3dbnode_filepathprefix,
            writeBufferSize: 65536,
            infoReadBufferSize: 64,
            seekReadBufferSize: 4096,
            throughputLimitMbps: 100.0,
            throughputCheckEvery: 128,
        },
        repair: {   
            enabled: false,
            interval: "2h",
            offset: "30m",
            jitter: "1h",
            throttle: "2m",
            checkInterval: "1m",
        },
        pooling: {
            blockAllocSize: 16,
            type: globals.m3dbnode_pooltype,
            seriesPool: {
                size: 262144,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            blockPool: {
                size: 262144,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            encoderPool: {
                size: 262144,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            closersPool: {
                size: 104857,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            contextPool: {
                size: 262144,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            segmentReaderPool: {
                size: 16384,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            iteratorPool: {
                size: 2048,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            fetchBlockMetadataresultsPool: {
                size: 65536,
                capacity: 32,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            fetchBlocksMetadataresultsPool: {
                size: 32,
                capacity: 4096,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            hostBlockMetadataSlicePool: {
                size: 131072,
                capacity: 3,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            blockMetadataPool: {
                size: 65536,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            blockMetadataSlicePool: {
                size: 65536,
                capacity: 32,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            blocksMetadataPool: {
                size: 65536,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            blocksMetadataSlicePool: {
                size: 32,
                capcity: 4096,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            identifierPool: {
                size: 262144,
                lowWatermark: "0.7",
                highWatermark: "1.0",
            },
            // next is bytes poo
            bytesPool: {
                buckets: [
                    {
                        capacity: 16,
                        size: 524288,
                        lowWatermark: "0.7",
                        highWatermark: "1.0",
                    },
                    {
                        capacity: 32,
                        size: 262144,
                        lowWatermark: "0.7",
                        highWatermark: "1.0",
                    },
                    {
                        capacity: 64,
                        size: 131072,
                        lowWatermark: "0.7",
                        highWatermark: "1.0",
                    },
                    {
                        capacity: 128,
                        size: 65536,
                        lowWatermark: "0.7",
                        highWatermark: "1.0",
                    },
                    {
                        capacity: 256,
                        size: 65536,
                        lowWatermark: "0.7",
                        highWatermark: "1.0",
                    },
                    {
                        capacity: 1440,
                        size: 16384,
                        lowWatermark: "0.7",
                        highWatermark: "1.0",
                    },
                    {
                        capacity: 4096,
                        size: 8192,
                        lowWatermark: 0.7,
                        highWatermark: "1.0",
                    },
                ],             
            },
        },
        logging: {
            level: "info",
        },
        // this might not be needed in the cloud
        metrics: {
            m3: {
                hostPort: globals.m3_address,
                service: "m3dbnode",
                env: "cloud",
                includeHost: true,
            },
            samplingRate: 0.01,
            extended: "moderate",
            sanitization: "m3",
        },
        config: {
            service: {
                env: globals.kv_env,
                zone: globals.kv_dc,
                service: "m3dbnode",
                cacheDir: "/var/lib/m3kv",
                etcdClusters: [
                    if cluster.zone == globals.kv_dc then
                    {
                        zone: cluster.zone,
                        endpoints: cluster.endpoints
                    },// this doesn't execute cleanly rn 
                    for cluster in globals.etcd.clusters
                ],
            }        
        },
    },
})
