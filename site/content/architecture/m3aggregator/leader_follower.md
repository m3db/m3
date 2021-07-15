---
title: "Leader & Follower"
weight: 2
---

A single `m3aggregator` node for [every shardset](https://github.com/m3db/m3/blob/c2dc8fe548790aed16ac88d498f3e5046c76dda6/src/aggregator/aggregator/election_mgr.go#L336) 
is elected to be a leader. Both leader and
follower nodes are receiving the writes and performing the aggregation. 
The main difference between the leader and the follower is that the leader node
is responsible for flushing (persisting) the data it has aggregated 
(see [Flushing](/docs/architecture/m3aggregator/flushing) for more details).
The follower is standing by ready to take over flushing in case the current leader fails.

Election state of the node can be checked by using `/status` endpoint:

    curl http://localhost:6001/status

```json
{
  "status": {
    "flushStatus": {
      "electionState": "follower",
      "canLead": true
    }
  }
}
```

`canLead` field is set to `true` on the follower node when that node is ready to take 
leader role without data loss. For that, the node must have accumulated all the data since
the last flush done by the current leader (for the shardset that it owns).

`canLead` is always `true` on leader nodes. 
