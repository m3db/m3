---
title: "Troubleshooting"
weight: 8
chapter: true
---

Some common problems and resolutions

## Ports 9001-9004 aren't open after starting m3db.

These ports will not open until a namespace and placement have been created and the nodes have bootstrapped.

## Bootstrapping is slow

Double check your configuration against the [bootstrapping guide](/docs/operational_guide/bootstrapping_crash_recovery). The nodes will log what bootstrapper they are using and what time range they are using it for.

If you're using the commitlog bootstrapper, and it seems to be slow, ensure that snapshotting is enabled for your namespace. Enabling snapshotting will require a node restart to take effect.

If an m3db node hasn't been able to snapshot for awhile, or is stuck in the commitlog bootstrapping phase for a long time due to accumulating a large number of commitlogs, consider using the peers bootstrapper. In situations where a large number of commitlogs need to be read, the peers bootstrapper will outperform the commitlog bootstrapper (faster and less memory usage) due to the fact that it will receive already-compressed data from its peers. Keep in mind that this will only work with a replication factor of 3 or larger and if the nodes peers are healthy and bootstrapped. Review the [bootstrapping guide](/docs/operational_guide/bootstrapping_crash_recovery) for more information.

## Nodes a crashing with memory allocation errors, but there's plenty of available memory

Ensure you've set `vm.max_map_count` to something like 262,144 using sysctl. Find out more in the [Clustering the Hard Way](/docs/operational_guide/kernel_configuration) document.

## What to do if my M3DB node is OOM’ing?

1.  Ensure that you are not co-locating coordinator, etcd or query nodes with your M3DB nodes. Colocation or embedded mode is fine for a development environment, but highly discouraged in production.
2.  Check to make sure you are running adequate block sizes based on the retention of your namespace. See [namespace configuration](/docs/operational_guide/namespace_configuration) for more information.
3.  Ensure that you use at most 50-60% memory utilization in the normal running state. You want to ensure enough overhead to handle bursts of metrics, especially ones with new IDs as those will take more memory initially.
4.  High cardinality metrics can also lead to OOMs especially if you are not adequately provisioned. If you have many unique timeseries such as ones containing UUIDs or timestamps as tag values, you should consider mitigating their cardinality.

## Using the /debug/dump API

The `/debug/dump` API returns a number of helpful debugging outputs. Currently, we support the following:

-   CPU profile: determines where a program spends its time while actively consuming CPU cycles (as opposed to while sleeping or waiting for I/O). Currently set to take a 5 second profile.
-   Heap profile: reports memory allocation samples; used to monitor current and historical memory usage, and to check for memory leaks.
-   Goroutines profile: reports the stack traces of all current goroutines.
-   Host profile: returns data about the underlying host such as PID, working directory, etc.
-   Namespace: returns information about the namespaces setup in M3DB
-   Placement: returns information about the placement setup in M3DB

This endpoint can be used on both the db nodes as well as the coordinator/query nodes. However, namespace and placement info are only available on the coordinator debug endpoint.

To use this, simply run the following on either the M3DB debug listen port or the regular port on M3Coordinator.

You may need to include the following headers:
{{% fileinclude file="headers_placement_namespace.md" %}}

    curl -H "Cluster-Environment-Name: [set if not default]" <m3db_or_m3coordinator_ip>:<port>/debug/dump > <tmp_zip_file.zip>

    # unzip the file
    unzip <tmp_zip_file.zip>

Now, you will have the following files, which you can use for troubleshooting using the below commands:

**cpuSource**

    go tool pprof -http=:16000 cpu.prof

**heapSource**

    go tool pprof -http=:16000 heap.prof

**goroutineProfile**

    less goroutine.prof

**hostSource**

    less host.json | jq .

**namespaceSource**

    less namespace.json | jq .

**placementSource**

    less placement-m3db.json | jq .