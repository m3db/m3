---
title: "M3 Coordinator"
weight: 3
---

M3 Coordinator is a service that coordinates reads and writes between upstream systems, such as Prometheus, and downstream systems, such as M3DB.

It also provides management APIs to setup and configure different parts of M3.

The coordinator is generally a bridge for read and writing different types of metrics formats and a management layer for M3.

**Note**: M3DB by default includes the M3 Coordinator accessible on port 7201. For production deployments it is recommended to deploy it as a dedicated service to ensure you can scale the write coordination role separately and independently to database nodes as an isolated application separate from the M3DB database role.
