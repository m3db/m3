---
title: "Architecture"
weight: 2
---

## Overview

M3DB is written entirely in Go and does not have any required dependencies. For larger deployments, one may use an etcd cluster to manage M3DB cluster membership and topology definition.

## High Level Goals

Some of the high level goals for the project are defined as:

-   **Monitoring support:** M3DB was primarily developed for collecting a high volume of monitoring time series data, distributing the storage in a horizontally scalable manner and most efficiently leveraging the hardware.  As such time series that are not read frequently are not kept in memory.
-   **Highly configurable:** Provide a high level of configuration to support a wide set of use cases and runtime environments.
-   **Variable durability:** Providing variable durability guarantees for the write and read side of storing time series data enables a wider variety of applications to use M3DB. This is why replication is primarily synchronous and is provided with configurable consistency levels, to enable consistent writes and reads. It must be possible to use M3DB with strong guarantees that data was replicated to a quorum of nodes and that the data was durable if desired.
