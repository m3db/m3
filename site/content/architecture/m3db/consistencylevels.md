---
title: "Consistency Levels"
weight: 3
---

M3DB provides variable consistency levels for read and write operations, as well as cluster connection operations. These consistency levels are handled at the client level.

## Write consistency levels

-   **One:** Corresponds to a single node succeeding for an operation to succeed.
-   **Majority:** Corresponds to the majority of nodes succeeding for an operation to succeed.
-   **All:** Corresponds to all nodes succeeding for an operation to succeed.

## Read consistency levels

-   **One**: Corresponds to reading from a single node to designate success.
-   **UnstrictMajority**: Corresponds to reading from the majority of nodes but relaxing the constraint when it cannot be met, falling back to returning success when reading from at least a single node after attempting reading from the majority of nodes.
-   **Majority**: Corresponds to reading from the majority of nodes to designate success.
-   **All:** Corresponds to reading from all of the nodes to designate success.

## Connect consistency levels

Connect consistency levels are used to determine when a client session is deemed as connected before operations can be attempted.

-   **Any:** Corresponds to connecting to any number of nodes for all shards, this strategy will attempt to connect to all, then the majority, then one and then fallback to none and as such will always succeed.
-   **None:** Corresponds to connecting to no nodes for all shards and as such will always succeed.
-   **One:** Corresponds to connecting to a single node for all shards.
-   **Majority:** Corresponds to connecting to the majority of nodes for all shards.
-   **All:** Corresponds to connecting to all of the nodes for all shards.
