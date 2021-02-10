---
title: "Peer Streaming"
weight: 6
---

## Client

Peer streaming is managed by the M3DB client. It fetches all blocks from peers for a specified time range for bootstrapping purposes. It performs the following steps:

1.  Fetch all metadata for blocks from all peers who own the specified shard
2.  Compares metadata from different peers and determines the best peer(s) from which to stream the actual data
3.  Streams the block data from peers

Steps 1, 2 and 3 all happen concurrently. As metadata streams in, we begin determining which peer is the best source to stream a given block's data for a given series from, and then we begin streaming data from that peer while we continue to receive metadata. If the checksum for a given series block matches all three replicas then the least loaded (in terms of outstanding requests) and recently attempted will be selected to stream from. If the checksum differs for the series block across any of the peers then a fanout fetch of the series block is performed.

In terms of error handling, the client will respect the consistency level specified for bootstrap. This means that when fetching metadata, indefinite retry is performed until the consistency level is achieved, for instance for quorum a majority of peers must successfully return metadata. For fetching the block data, if checksum matches from all peers then one successful fetch must occur, unless bootstrap consistency level "none" is specified, and if checksum mismatches then the specified consistency level must be achieved when the series block fetch is fanned out to peers. Fetching block data as well will indefinitely retry until the consistency level is achieved.

The client supports dynamically changing the bootstrap consistency level, which is helpful in disaster scenarios where the consistency level cannot be achieved. To break the indefinite streaming attempt an operator can change the consistency level to "none" and a purely best-effort will be made to fetch the metadata and correspondingly to fetch the block data.

The diagram below depicts the control flow and concurrency (goroutines and channels) in detail:

                 ┌───────────────────────────────────────────────┐
                 │                                               │
                 │         FetchBootstrapBlocksFromPeers         │
                 │                                               │
                 └───────────────────────────────────────────────┘
                                         │
                                         │
                    ┌────────────────────┘
                    │
                    ▼
    ┌───────────────────────────────┐
    │         Main routine          │
    │                               │
    │     1) Create metadataCh      │────────────────┐
    │ 2) Spin up background routine │                │
    └───────────────────────────────┘      Create with metadataCh
                    │                                │
                    │                                ▼
                    │                ┌───────────────────────────────┐
                    │                │                               │
                    │                │      Background routine       │
                    │                │                               │
                    │                └───────────────────────────────┘
                    │                                │
                    │                          For each peer
                    │                                │
                    │               ┌────────────────┼─────────────────┐
                    │               │                │                 │
                    │               │                │                 │
                    │               ▼                ▼                 ▼
                    │          ┌───────────────────────────────────────────┐
                    │          │       StreamBlocksMetadataFromPeer        │
                    │          │                                           │
                    │          │  Stream paginated blocks metadata from a  │
                    │          │        peer while pageToken != nil        │
                    │          │                                           │
                    │          │     For each blocks' metadata --> put     │
                    │          │         metadata into metadataCh          │
                    │          └───────────────────────────────────────────┘
                    ▼
    ┌───────────────────────────────────────────┐
    │           StreamBlocksFromPeers           │
    │                                           │
    │ 1) Create a background goroutine (details │
    │               to the right)               │
    │                                           │
    │ 2) Create a queue per-peer which each have│
    │   their own internal goroutine and will   │
    │   stream blocks back per-series from a    │──────────┐
    │              specific peer                │          │
    │                                           │          │
    │ 3) Loop through the enqueCh and pick an   │ Creates with metadataCh
    │appropriate peer(s) for each series (based │     and enqueueCh
    │on whether all the peers have the same data│          │
    │ or not) and then put that into the queue  │          │
    │for that peer so the data will be streamed │          │
    └───────────────────────────────────────────┘          │
                    │                                      ▼
                    │    ┌──────────────────────────────────────────────────────────┐
                    │    │   streamAndGroupCollectedBlocksMetadata (injected via    │
                    │    │                streamMetadataFn variable)                │
                    │    │                                                          │
                    │    │ Loop through the metadataCh aggregating blocks metadata  │
                    │    │per series/block combination from different peers until we│
                    │    │   have them from all peers for a series/block metadata   │
                    │    │   combination and then "submit" them to the enqueueCh    │
                    │    │                                                          │
                    │    │At the end, flush any remaining series/block combinations │
                    │    │(that we received from less than N peers) into the enqueCh│
                    │    │                         as well.                         │
                    │    └──────────────────────────────────────────────────────────┘
                    │
              For each peer
                    │
       ┌────────────┼─────────────┐
       │            │             │
       │            │             │
       ▼            ▼             ▼
    ┌─────────────────────────────────────────────────────────────┐
    │ newPeerBlocksQueue (processFn = streamBlocksBatchFromPeer)  │
    │                                                             │
    │For each peer we're creating a new peerBlocksQueue which will│
    │     stream data blocks from a specific peer (using the      │
    │   streamBlocksBatchFromPeer function) and add them to the   │
    │                        blocksResult                         │
    │                                                             │
    └─────────────────────────────────────────────────────────────┘
