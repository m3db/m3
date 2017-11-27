# Architecture

## Overview

M3DB is written entirely in Go and has a single dependency of etcd which is used for cluster membership and topology definition. In the near future we plan to also support the concept of seed nodes that will run an embedded etcd server, as etcd can be run in an embedded fashion reasonably simply and statically compiled into the M3DB binary itself.

## High Level Goals

Some of the high level goals for the project are defined as:

* **Configurable durability:** Providing configurable durability guarentees for the write and read side of storing time series data enables a wider variety of applications to use M3DB. This is why replication is primarily synchronous and is provided with configurable consistency levels, to enable consistent writes and reads. It must be possible to use M3DB with strong guarentees that data was replicated to a quorum of nodes and that the data was durable if desired.
