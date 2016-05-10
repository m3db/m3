namespace java com.memtsdb.node.benchmarks

service MemTSDB {
	FetchResult fetch(1: FetchRequest req)
	FetchBatchResult fetchBatch(1: FetchBatchRequest req)
}

struct FetchRequest {
	1: required i64 startUnixMs
	2: required i64 endUnixMs
	3: required string id
}

struct Segment {
	1: required binary value
}

struct FetchResult {
	1: required list<Segment> segments
}

struct FetchBatchRequest {
	1: required i64 startUnixMs
	2: required i64 endUnixMs
	3: required list<string> ids
}

struct FetchBatchResult {
	1: required list<FetchResult> results
}
