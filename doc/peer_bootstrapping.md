## M3DB Peer Bootstrapping

### Client code

Peer bootstrapping from the client begins with the FetchBootstrapBlocksFromPeers function.

FetchBootstrapBlocksFromPeers will fetch the specified blocks from peers for
bootstrapping purposes. It does the following:
1) Fetch all metadata blocks from all peers who own the specified shard
2) Compares metadata from different peers and determines the best peer(s)
  from which to stream the actual data
3) Streams the data from peers

Note that steps 1, 2, and 3 all happen conccurently I.E as metadata streams
in, we begin determining which peer is the best source to stream the rest of
of the data from, and then we will begin streaming data from that peer all
the while we continue to receive metadata.

In terms of error handling, if an error occurs during the metadata streaming
portion, then this function will return an error. However, if something goes
wrong during the data streaming portion, it will not return an error, and
the function will just return as much data as it can.

The diagram below depicts the control flow and concurrency (goroutines and channels) in detail:

```
                                 ┌───────────────────────────────────────────────┐
                                 │                                               │
                                 │                                               │
                                 │         FetchBootstrapBlocksFromPeers         │
                                 │                                               │
                                 │                                               │
                                 └───────────────────────────────────────────────┘
                                                         │
                                                         │
                              ┌──────────────────────────┴───────────────────────────┐
                              │                                                      │
                              ▼                                                      ▼
              ┌───────────────────────────────┐                      ┌───────────────────────────────┐
              │         Main routine          │                      │                               │
              │                               │       Creates        │      Background routine       │
              │     1) Create metadataCh      │────────(pass ───────▶│                               │
              │ 2) Spin up background routine │     metadataCh)      │                               │
              └───────────────────────────────┘                      └───────────────────────────────┘
                              │                                                 For each peer
                              │                                                      │
                              │                                     ┌────────────────┼─────────────────┐
                              │                                     │                │                 │
                              │                                     │                │                 │
                              │                                     ▼                ▼                 ▼
                              │                                  ┌───────────────────────────────────────────┐
                              │                                  │       StreamBlocksMetadataFromPeer        │
                              │                                  │                                           │
                              │                                  │  Stream paginated blocks metadata from a  │
                              │                                  │        peer while pageToken != nil        │
                              │                                  │                                           │
                              │                                  │ For each blocks metadata --> put metadata │
                              │                                  │              into metadataCh              │
                              │                                  └───────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────────┐
        │           StreamBlocksFromPeers           │
        │                                           │                                     ┌──────────────────────────────────────────────────────────┐
        │ 1) Create a background goroutine (details │                                     │   streamAndGroupCollectedBlocksMetadata (injected via    │
        │               to the right)               │                                     │                streamMetadataFn variable)                │
        │                                           │                                     │                                                          │
        │2) Create a queue per-peer which each have │                                     │ Loop through the metadataCh aggregating blocks metadata  │
        │   their own internal goroutine and will   │    Creates (pass metadataCh         │per series/block combination from different peers until we│
        │   stream blocks back per-series from a    │─────────and enqueueCh)─────────────▶│   have them from all peers for a series/block metadata   │
        │              specific peer.               │                                     │   combination and then "submit" them to the enqueueCh    │
        │                                           │                                     │                                                          │
        │  3) Loop through the enqueCh and pick an  │                                     │At the end, flush any remaining series/block combinations │
        │appropriate peer(s) for each series (based │                                     │(that we received from less than N peers) into the enqueCh│
        │on whether all the peers have the same data│                                     │                         as well.                         │
        │ or not) and then put that into the queue  │                                     └──────────────────────────────────────────────────────────┘
        │for that peer so the data will be streamed │
        └───────────────────────────────────────────┘
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
```