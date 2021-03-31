---
title: "Caching"
weight: 7
---

## Overview

Blocks that are still being actively compressed / M3TSZ encoded must be kept in memory until they are sealed and flushed to disk. Blocks that have already been sealed, however, don't need to remain in-memory. In order to support efficient reads, M3DB implements various caching policies which determine which flushed blocks are kept in memory, and which are not. The "cache" itself is not a separate datastructure in memory, cached blocks are simply stored in their respective [in-memory objects](/docs/architecture/m3db/engine#in-memory-object-layout) with various different mechanisms (depending on the chosen cache policy) determining which series / blocks are evicted and which are retained.

For general purpose workloads, the `lru` caching policy is reccommended.

## None Cache Policy

The `none` cache policy is the simplest. As soon as a block is sealed, its flushed to disk and never retained in memory again. This cache policy will have the lowest memory consumption, but also the poorest read performance as every read for a block that is already flushed will require a disk read.

## All Cache Policy

The `all` cache policy is the opposite of the `none` cache policy. All blocks are kept in memory until their retention period is over. This policy can be useful for read-heavy workloads with small datasets, but is obviously limited by the amount of memory on the host machine. Also keep in mind that this cache policy may have unintended side-effects on write throughput as keeping every block in memory creates a lot of work for the Golang garbage collector.

## Recently Read Cache Policy

The `recently_read` cache policy keeps all blocks that are read from disk in memory for a configurable duration of time. For example, if the `recently_read` cache policy is set with a duration of 10 minutes, then everytime a block is read from disk it will be kept in memory for at least 10 minutes. This policy can be very effective if only a small portion of your overall dataset is ever read, and especially if that subset is read frequently (i.e as is common in the case of database backing an automatic alerting system), but it can cause very high memory usage during workloads that involve sequentially scanning all of the data.

Data eviction from memory is triggered by the "ticking" process described in the [background processes section](/docs/architecture/m3db/engine#background-processes)

## Least Recently Used (LRU) Cache Policy

The `lru` cache policy uses an `lru` list with a configurable max size to keep track of which blocks have been read least recently, and evicts those blocks first when the capacity of the list is full and a new block needs to be read from disk. This cache policy strikes the best overall balance and is the recommended policy for general case workloads. Review the comments in `wired_list.go` for implementation details.
