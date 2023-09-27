`verify_ids` is a small tool that fetches a number of IDs (per given shard),
and compares the data for those IDs over a limited time range, bypassing
all M3 client clustering logic and accessing dbnodes directly via Node TChannel API.

The first node in the list will be used as a reference node to fetch sample IDs
for the given time range, if not using `-stdin` flag to provide a list of IDs to query.

Then all nodes will be queried (again) via a constructed conjunction query for
the data itself. The results may include a longer time frame, as we're receiving
block(s) worth of data that's not truncated to requested time boundary.

The data comparison is simplistic, but you can request raw query results via `-raw` flag
for manual analysis.

Examples:
```
# ./verify_ids -nodes=127.0.0.1:9000,10.24.141.38:9000 -num=4 -shards=0,6

# echo '{__name__="coordinator_ingest_latency_bucket",handler="remote_write",instance="localhost:7203"}' | \
./verify_ids -nodes 10.24.144.134:9000,10.24.149.166:9000 -stdin -raw | \
jq .

```
