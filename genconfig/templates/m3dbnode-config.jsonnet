// we can set cluster as a top level argument when running jsonnet
local cluster = std.extVar("cluster");

// we need to import all environments bc import paths cannot be generated
local environments = {
    "example": (import "../environments/example.libsonnet"),
    "gcp": (import "../environments/gcp.libsonnet"),
};

// we can loop through multiple environments if we want
local env_vars = environments[cluster];
local globals = env_vars.globals + env_vars.m3dbnode.globals;

std.prune({
    coordinator: {
        listenAddress: "0.0.0.0:" + globals.coordinator_port,
        metrics: { 
            scope: {
                prefix: "coordinator",
            },
            prometheus: {
                handlerPath: globals.metrics_handler_path,
                listenAddress: "0.0.0.0:" + (globals.coordinator_port+2) 
            },
            sanitization: globals.metrics_sanitization,
            samplingRate: globals.metrics_sampling_rate,
            extended: "none", 
        },
    },

    db: {
        listenAddress: "0.0.0.0:" + globals.port,
        clusterListenAddress: "0.0.0.1:" + (globals.port+1),
        httpClusterListenAddress: "0.0.0.0:" + (globals.port+2),
        httpNodeListenAddress: "0.0.0.0:" + (globals.port+3),
        debugListenAddress: "0.0.0.0:" + (globals.port+4),
        hostId: {
            resolver: "config",
            value: "m3db_local",
        },
        client: {
            writeConsistencyLevel: "majority",
            readConsistencyLevel: "unstrict_majority",
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
                backoffFactor: 2,
                maxRetris: 3,
                jitter: true,   
            },
            backgroundHealthCheckFailLimit: 4,
            backgroundHealthCheckFailThrottleFactor: 0.5, 
        }, 
        gcPercentage: globals.m3dbnode_gcpercentage,
        writeNewSeriesAsync: true,
        writeNewSeriesLimitPerSecond: 1048576, // 2 ^ 20 
        writeNewSeriesBackoffDuration: "2ms",

        // continue here
        bootstrap: {
            bootstrappers: [
                "filesystem",
                "commitlog",
            ],
            fs: {
                numProcessorsPerCPU: 0.125 // 4 cpus for 32 node cpu
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
            dataReadBufferSize: 65536,
            infoReadBufferSize: 128,
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
        metrics: {
            prometheus: {
                handlerPath: globals.metrics_handler_path,
            },
            samplingRate: globals.metrics_sampling_rate,
            extended: "detailed",
            sanitization: globals.metrics_sanitization,
        },
        config: {
            service: {
                env: globals.env,
                zone: globals.zone,
                service: "m3db",
                cacheDir: "/var/lib/m3kv",
                etcdClusters: [
                    if cluster.zone == globals.zone then
                    {
                        zone: cluster.zone,
                        endpoints: cluster.endpoints
                    },
                    for cluster in globals.etcd.clusters
                ],
            },
            seedNodes: {
                initialCluster: [
                    {
                        hostId: "m3db_local",
                        endpoint: "http://127.0.0.1:2380",
                    },
                ],
            }, 
        },
    },
})
