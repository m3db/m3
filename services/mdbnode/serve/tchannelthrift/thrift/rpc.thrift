namespace java com.memtsdb.mdbnode

enum TimeType {
	UNIX_SECONDS,
	UNIX_MICROSECONDS,
	UNIX_MILLISECONDS,
	UNIX_NANOSECONDS
}

enum NodeErrorType {
	INTERNAL_ERROR,
	BAD_REQUEST
}

exception NodeError {
	1: required NodeErrorType type = NodeErrorType.INTERNAL_ERROR
	2: required string message
}

exception WriteError {
	1: required NodeErrorType type = NodeErrorType.INTERNAL_ERROR
	2: required string message
}
exception WriteBatchErrors {
	1: required list<WriteBatchError> errors
}

service Node {
	void write(1: WriteRequest req) throws (1: WriteError err)
	void writeBatch(1: WriteBatchRequest req) throws (1: WriteBatchErrors err)
	FetchResult fetch(1: FetchRequest req) throws (1: NodeError err)
	FetchRawBatchResult fetchRawBatch(1: FetchRawBatchRequest req) throws (1: NodeError err)
}

struct WriteRequest {
	1: required string id
	2: required Datapoint datapoint
}

struct WriteBatchRequest {
	1: required list<WriteRequest> elements
}

struct WriteBatchError {
	1: required i64 elementErrorIndex
	2: required WriteError error
}

struct FetchRequest {
	1: required i64 rangeStart
	2: required i64 rangeEnd
	3: required string id
	4: optional TimeType rangeType = TimeType.UNIX_SECONDS
	5: optional TimeType resultTimeType = TimeType.UNIX_SECONDS
}

struct FetchResult {
	1: required list<Datapoint> datapoints
}

struct Datapoint {
	1: required i64 timestamp
	2: required double value
	3: optional binary annotation
	4: optional TimeType timestampType = TimeType.UNIX_SECONDS
} 

struct FetchRawBatchRequest {
	1: required i64 rangeStart
	2: required i64 rangeEnd
	3: required list<string> ids
	4: optional TimeType rangeType = TimeType.UNIX_SECONDS
}

struct FetchRawBatchResult {
	1: required list<FetchRawResult> elements
}

struct Segment {
    1: required binary head
    2: required binary tail
}

struct FetchRawResult {
	1: required list<Segment> segments
}
