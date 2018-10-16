# Troubleshooting

Some common problems and resolutions

## Ports 9001-9004 aren't open after starting m3db.

These ports will not open until a namespace and placement have been created and the nodes have bootstrapped.

## Bootstrapping is slow

Double check your configuration against the [bootstrapping guide](../operational_guide/bootstrapping.md). The nodes will log what bootstrapper they are using and what time range they are using it for.

If you don't seem to be using the filesystem bootstrapper, ensure that snapshotting is enabled for your namespace.

If your node has been unable to snapshot for a long period of time, and is stuck in the commitlog bootstrapper, the peers bootstrapper may help as it uses less memory. This will only work if you have other nodes with replicas to peer bootstrap from.

## Things aren't scaling well and there are weird memory issues

Ensure you've set `vm.max_map_count` to something like 262,144 using sysctl. Find out more in the [Clustering the Hard Way](../how_to/cluster_hard_way.md#kernel) document.
