# Troubleshooting

Some common problems and resolutions

## Ports 9001-9004 aren't open after starting m3db.

These ports will not open until a namespace and placement have been created and the nodes have bootstrapped.

## Bootstrapping is slow

Double check your configuration against the [bootstrapping guide](../operational_guide/bootstrapping.md). The nodes will log what bootstrapper they are using and what time range they are using it for.

If you're using the commitlog bootstrapper, and it seems to be slow, ensure that snapshotting is enabled for your namespace. Enabling snapshotting will require a node restart to take effect.

If an m3db node hasn't been able to snapshot for awhile, or is stuck in the commitlog bootstrapping phase for a long time due to accumulating a large number of commitlogs, consider using the peers bootstrapper. In situations where a large number of commitlogs need to be read, the peers bootstrapper will outperform the commitlog bootstrapper (faster and less memory usage) due to the fact that it will receive already-compressed data from its peers. Keep in mind that this will only work with a replication factor of 3 or larger and if the nodes peers are healthy and bootstrapped. Review the [bootstrapping guide](../operational_guide/bootstrapping.md) for more information.

## Nodes a crashing with memory allocation errors, but there's plenty of available memory

Ensure you've set `vm.max_map_count` to something like 262,144 using sysctl. Find out more in the [Clustering the Hard Way](../how_to/cluster_hard_way.md#kernel) document.
