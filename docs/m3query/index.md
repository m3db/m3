# M3 Query, a statelees query server for M3DB and Prometheus

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
