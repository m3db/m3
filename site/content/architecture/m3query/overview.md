---
title: "Overview"
weight: 1
---

M3 Query and M3 Coordinator are written entirely in Go, M3 Query is as a query engine for [M3DB](https://m3db.io/) and M3 Coordinator is a remote read/write endpoint for Prometheus and M3DB. To learn more about Prometheus's remote endpoints and storage, [see here](https://prometheus.io/docs/operating/integrations/#remote-endpoints-and-storage).

M3 Query is a service that exposes all metrics query endpoints along with 
metrics time series metadata APIs that return dimensions and labels of metrics
that reside in a M3DB cluster.

**Note**: M3 Coordinator, and by proxy M3DB, by default includes the M3 
Query endpoints accessible on port 7201.
For production deployments it is recommended to deploy it as a 
dedicated service to ensure you can scale the memory heavy query role separately 
from the metrics ingestion write path of writes through M3 Coordinator to M3DB
database role nodes. This allows excessive queries to primarily affect the 
dedicated M3 Query service instead of interrupting service to the write 
ingestion pipeline.
