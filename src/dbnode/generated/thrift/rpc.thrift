// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

namespace java com.github.m3db

enum TimeType {
	UNIX_SECONDS,
	UNIX_MICROSECONDS,
	UNIX_MILLISECONDS,
	UNIX_NANOSECONDS
}

enum ErrorType {
	INTERNAL_ERROR,
	BAD_REQUEST
}

enum ErrorFlags {
    NONE               = 0x00,
    RESOURCE_EXHAUSTED = 0x01
}

exception Error {
	1: required ErrorType type = ErrorType.INTERNAL_ERROR
	2: required string message
	3: optional i64 flags = 0
}

exception WriteBatchRawErrors {
	1: required list<WriteBatchRawError> errors
}

service Node {
	// Friendly not highly performant read/write endpoints
	QueryResult          query(1: QueryRequest req) throws (1: Error err)
	AggregateQueryResult aggregate(1: AggregateQueryRequest req) throws (1: Error err)
	FetchResult          fetch(1: FetchRequest req) throws (1: Error err)
	void                 write(1: WriteRequest req) throws (1: Error err)
	void                 writeTagged(1: WriteTaggedRequest req) throws (1: Error err)

	// Performant read/write endpoints
	AggregateQueryRawResult        aggregateRaw(1: AggregateQueryRawRequest req) throws (1: Error err)
	FetchBatchRawResult            fetchBatchRaw(1: FetchBatchRawRequest req) throws (1: Error err)
	FetchBatchRawResult            fetchBatchRawV2(1: FetchBatchRawV2Request req) throws (1: Error err)
	FetchBlocksRawResult           fetchBlocksRaw(1: FetchBlocksRawRequest req) throws (1: Error err)
	FetchTaggedResult              fetchTagged(1: FetchTaggedRequest req) throws (1: Error err)
	FetchBlocksMetadataRawV2Result fetchBlocksMetadataRawV2(1: FetchBlocksMetadataRawV2Request req) throws (1: Error err)
	void                           writeBatchRaw(1: WriteBatchRawRequest req) throws (1: WriteBatchRawErrors err)
	void                           writeBatchRawV2(1: WriteBatchRawV2Request req) throws (1: WriteBatchRawErrors err)
	void                           writeTaggedBatchRaw(1: WriteTaggedBatchRawRequest req) throws (1: WriteBatchRawErrors err)
	void                           writeTaggedBatchRawV2(1: WriteTaggedBatchRawV2Request req) throws (1: WriteBatchRawErrors err)
	void                           repair() throws (1: Error err)
	TruncateResult                 truncate(1: TruncateRequest req) throws (1: Error err)

	AggregateTilesResult aggregateTiles(1: AggregateTilesRequest req) throws (1: Error err)

	// Management endpoints
	NodeHealthResult                               health() throws (1: Error err)
	// NB: bootstrapped is for use with cluster management tools like k8s.
	NodeBootstrappedResult                         bootstrapped() throws (1: Error err)
	// NB: bootstrappedInPlacementOrNoPlacement is for use with cluster management tools like k8s.
	NodeBootstrappedInPlacementOrNoPlacementResult bootstrappedInPlacementOrNoPlacement() throws (1: Error err)
	NodePersistRateLimitResult                     getPersistRateLimit() throws (1: Error err)
	NodePersistRateLimitResult                     setPersistRateLimit(1: NodeSetPersistRateLimitRequest req) throws (1: Error err)
	NodeWriteNewSeriesAsyncResult                  getWriteNewSeriesAsync() throws (1: Error err)
	NodeWriteNewSeriesAsyncResult                  setWriteNewSeriesAsync(1: NodeSetWriteNewSeriesAsyncRequest req) throws (1: Error err)
	NodeWriteNewSeriesBackoffDurationResult        getWriteNewSeriesBackoffDuration() throws (1: Error err)
	NodeWriteNewSeriesBackoffDurationResult        setWriteNewSeriesBackoffDuration(1: NodeSetWriteNewSeriesBackoffDurationRequest req) throws (1: Error err)
	NodeWriteNewSeriesLimitPerShardPerSecondResult getWriteNewSeriesLimitPerShardPerSecond() throws (1: Error err)
	NodeWriteNewSeriesLimitPerShardPerSecondResult setWriteNewSeriesLimitPerShardPerSecond(1: NodeSetWriteNewSeriesLimitPerShardPerSecondRequest req) throws (1: Error err)

	// Debug endpoints
	DebugProfileStartResult        debugProfileStart(1: DebugProfileStartRequest req) throws (1: Error err)
	DebugProfileStopResult         debugProfileStop(1: DebugProfileStopRequest req) throws (1: Error err)
	DebugIndexMemorySegmentsResult debugIndexMemorySegments(1: DebugIndexMemorySegmentsRequest req) throws (1: Error err)
}

struct FetchRequest {
	1: required i64 rangeStart
	2: required i64 rangeEnd
	3: required string nameSpace
	4: required string id
	5: optional TimeType rangeType = TimeType.UNIX_SECONDS
	6: optional TimeType resultTimeType = TimeType.UNIX_SECONDS
	7: optional binary source
}

struct FetchResult {
	1: required list<Datapoint> datapoints
}

struct Datapoint {
	1: required i64 timestamp
	2: required double value
	3: optional binary annotation
	4: optional TimeType timestampTimeType = TimeType.UNIX_SECONDS
}

struct WriteRequest {
	1: required string nameSpace
	2: required string id
	3: required Datapoint datapoint
}

struct WriteTaggedRequest {
	1: required string nameSpace
	2: required string id
	3: required list<Tag> tags
	4: required Datapoint datapoint
}

struct FetchBatchRawRequest {
	1: required i64 rangeStart
	2: required i64 rangeEnd
	3: required binary nameSpace
	4: required list<binary> ids
	5: optional TimeType rangeTimeType = TimeType.UNIX_SECONDS
	6: optional binary source
}


struct FetchBatchRawV2Request {
	1: required list<binary> nameSpaces
	2: required list<FetchBatchRawV2RequestElement> elements
	3: optional binary source
}

struct FetchBatchRawV2RequestElement {
	1: required i64 nameSpace
	2: required i64 rangeStart
	3: required i64 rangeEnd
	4: required binary id
	5: optional TimeType rangeTimeType = TimeType.UNIX_SECONDS
}

struct FetchBatchRawResult {
	1: required list<FetchRawResult> elements
}

struct FetchRawResult {
	1: required list<Segments> segments
	2: optional Error err
}

struct Segments {
	1: optional Segment merged
	2: optional list<Segment> unmerged
}

struct Segment {
	1: required binary head
	2: required binary tail
	3: optional i64 startTime
	4: optional i64 blockSize
	5: optional i64 checksum
}

struct FetchTaggedRequest {
	1: required binary nameSpace
	2: required binary query
	3: required i64 rangeStart
	4: required i64 rangeEnd
	5: required bool fetchData
	6: optional i64 limit
	7: optional TimeType rangeTimeType = TimeType.UNIX_SECONDS
	8: optional bool requireExhaustive = true
	9: optional i64 docsLimit
	10: optional binary source
}

struct FetchTaggedResult {
	1: required list<FetchTaggedIDResult> elements
	2: required bool exhaustive
}

struct FetchTaggedIDResult {
	1: required binary id
	2: required binary nameSpace
	3: required binary encodedTags
	4: optional list<Segments> segments

	// Deprecated -- do not use.
	5: optional Error err
}

struct FetchBlocksRawRequest {
	1: required binary nameSpace
	2: required i32 shard
	3: required list<FetchBlocksRawRequestElement> elements
	4: optional binary source
}

struct FetchBlocksRawRequestElement {
	1: required binary id
	2: required list<i64> starts
}

struct FetchBlocksRawResult {
	1: required list<Blocks> elements
}

struct Blocks {
	1: required binary id
	2: required list<Block> blocks
}

struct Block {
	1: required i64 start
	2: optional Segments segments
	3: optional Error err
	4: optional i64 checksum
}

struct Tag {
	1: required string name
	2: required string value
}

struct FetchBlocksMetadataRawV2Request {
	1: required binary nameSpace
	2: required i32 shard
	3: required i64 rangeStart
	4: required i64 rangeEnd
	5: required i64 limit
	6: optional binary pageToken
	7: optional bool includeSizes
	8: optional bool includeChecksums
	9: optional bool includeLastRead
}

struct FetchBlocksMetadataRawV2Result {
	1: required list<BlockMetadataV2> elements
	2: optional binary nextPageToken
}

struct BlockMetadataV2 {
	1: required binary id
	2: required i64 start
	3: optional Error err
	4: optional i64 size
	5: optional i64 checksum
	6: optional i64 lastRead
	7: optional TimeType lastReadTimeType = TimeType.UNIX_SECONDS
	8: optional binary encodedTags
}

struct WriteBatchRawRequest {
	1: required binary nameSpace
	2: required list<WriteBatchRawRequestElement> elements
}

struct WriteBatchRawV2Request {
	1: required list<binary> nameSpaces
	2: required list<WriteBatchRawV2RequestElement> elements
}

struct WriteBatchRawRequestElement {
	1: required binary id
	2: required Datapoint datapoint
}

struct WriteBatchRawV2RequestElement {
	1: required binary id
	2: required Datapoint datapoint
	3: required i64 nameSpace
}

struct WriteTaggedBatchRawRequest {
	1: required binary nameSpace
	2: required list<WriteTaggedBatchRawRequestElement> elements
}

struct WriteTaggedBatchRawV2Request {
	1: required list<binary> nameSpaces
	2: required list<WriteTaggedBatchRawV2RequestElement> elements
}

struct WriteTaggedBatchRawRequestElement {
	1: required binary id
	2: required binary encodedTags
	3: required Datapoint datapoint
}

struct WriteTaggedBatchRawV2RequestElement {
	1: required binary id
	2: required binary encodedTags
	3: required Datapoint datapoint
	4: required i64 nameSpace
}

struct WriteBatchRawError {
	1: required i64 index
	2: required Error err
}

struct TruncateRequest {
	1: required binary nameSpace
}

struct TruncateResult {
	1: required i64 numSeries
}

struct NodeHealthResult {
	1: required bool ok
	2: required string status
	3: required bool bootstrapped
	4: optional map<string,string> metadata
}

struct NodeBootstrappedResult {}

struct NodeBootstrappedInPlacementOrNoPlacementResult {}

struct NodePersistRateLimitResult {
	1: required bool limitEnabled
	2: required double limitMbps
	3: required i64 limitCheckEvery
}

struct NodeSetPersistRateLimitRequest {
	1: optional bool limitEnabled
	2: optional double limitMbps
	3: optional i64 limitCheckEvery
}

struct NodeWriteNewSeriesAsyncResult {
	1: required bool writeNewSeriesAsync
}

struct NodeSetWriteNewSeriesAsyncRequest {
	1: required bool writeNewSeriesAsync
}

struct NodeWriteNewSeriesBackoffDurationResult {
	1: required i64 writeNewSeriesBackoffDuration
	2: required TimeType durationType
}

struct NodeSetWriteNewSeriesBackoffDurationRequest {
	1: required i64 writeNewSeriesBackoffDuration
	2: optional TimeType durationType = TimeType.UNIX_MILLISECONDS
}

struct NodeWriteNewSeriesLimitPerShardPerSecondResult {
	1: required i64 writeNewSeriesLimitPerShardPerSecond
}

struct NodeSetWriteNewSeriesLimitPerShardPerSecondRequest {
	1: required i64 writeNewSeriesLimitPerShardPerSecond
}

service Cluster {
	HealthResult health() throws (1: Error err)
	void write(1: WriteRequest req) throws (1: Error err)
	void writeTagged(1: WriteTaggedRequest req) throws (1: Error err)
	QueryResult query(1: QueryRequest req) throws (1: Error err)
	AggregateQueryResult aggregate(1: AggregateQueryRequest req) throws (1: Error err)
	FetchResult fetch(1: FetchRequest req) throws (1: Error err)
	TruncateResult truncate(1: TruncateRequest req) throws (1: Error err)
}

struct HealthResult {
	1: required bool ok
	2: required string status
}

enum AggregateQueryType {
	AGGREGATE_BY_TAG_NAME,
	AGGREGATE_BY_TAG_NAME_VALUE,
}

// AggregateQueryRawRequest comes from our desire to aggregate on incoming data.
// We currently only support retrieval of facets, but could extend this based on
// requirements in the future. Currently, the predominant use-cases are:
// (1) Given a filter query (optionally), return all known tag keys matching this restriction
// (2) Given a filter query (optionally), return all know tag key+values matching this restriction
// (3) For (1), (2) - filter results to a given set of keys
struct AggregateQueryRawRequest {
	1: required binary query
	2: required i64 rangeStart
	3: required i64 rangeEnd
	4: required binary nameSpace
	5: optional i64 limit
	6: optional list<binary> tagNameFilter
	7: optional AggregateQueryType aggregateQueryType = AggregateQueryType.AGGREGATE_BY_TAG_NAME_VALUE
	8: optional TimeType rangeType = TimeType.UNIX_SECONDS
	9: optional binary source
}

struct AggregateQueryRawResult {
	1: required list<AggregateQueryRawResultTagNameElement> results
	2: required bool exhaustive
}

struct AggregateQueryRawResultTagNameElement {
	1: required binary tagName
	2: optional list<AggregateQueryRawResultTagValueElement> tagValues
}

struct AggregateQueryRawResultTagValueElement {
	1: required binary tagValue
}

// AggregateQueryRequest is identical to AggregateQueryRawRequest save for using string instead of binary for types.
struct AggregateQueryRequest {
	1: optional Query query
	2: required i64 rangeStart
	3: required i64 rangeEnd
	4: required string nameSpace
	5: optional i64 limit
	6: optional list<string> tagNameFilter
	7: optional AggregateQueryType aggregateQueryType = AggregateQueryType.AGGREGATE_BY_TAG_NAME_VALUE
	8: optional TimeType rangeType = TimeType.UNIX_SECONDS
	9: optional binary source
}

struct AggregateQueryResult {
	1: required list<AggregateQueryResultTagNameElement> results
	2: required bool exhaustive
}

struct AggregateQueryResultTagNameElement {
	1: required string tagName
	2: optional list<AggregateQueryResultTagValueElement> tagValues
}

struct AggregateQueryResultTagValueElement {
	1: required string tagValue
}

// Query wrapper types for simple non-optimized query use
struct QueryRequest {
	1: required Query query
	2: required i64 rangeStart
	3: required i64 rangeEnd
	4: required string nameSpace
	5: optional i64 limit
	6: optional bool noData
	7: optional TimeType rangeType = TimeType.UNIX_SECONDS
	8: optional TimeType resultTimeType = TimeType.UNIX_SECONDS
	9: optional binary source
}

struct QueryResult {
	1: required list<QueryResultElement> results
	2: required bool exhaustive
}

struct QueryResultElement {
	1: required string id
	2: required list<Tag> tags
	3: required list<Datapoint> datapoints
}

struct TermQuery {
	1: required string field
	2: required string term
}

struct RegexpQuery {
	1: required string field
	2: required string regexp
}

struct NegationQuery {
	1: required Query query
}

struct ConjunctionQuery {
	1: required list<Query> queries
}

struct DisjunctionQuery {
	1: required list<Query> queries
}

struct AllQuery {}

struct FieldQuery {
	1: required string field
}

struct Query {
	1: optional TermQuery        term
	2: optional RegexpQuery      regexp
	3: optional NegationQuery    negation
	4: optional ConjunctionQuery conjunction
	5: optional DisjunctionQuery disjunction
	6: optional AllQuery         all
	7: optional FieldQuery       field
}

struct AggregateTilesRequest {
	1: required string sourceNamespace
	2: required string targetNamespace
	3: required i64 rangeStart
	4: required i64 rangeEnd
	5: required string step
	6: optional TimeType rangeType = TimeType.UNIX_SECONDS
}

struct AggregateTilesResult {
	1: required i64 processedTileCount
}

struct DebugProfileStartRequest {
	1: required string name
	2: required string filePathTemplate
	3: optional string interval
	4: optional string duration
	5: optional i64 debug
	6: optional i64 conditionalNumGoroutinesGreaterThan
	7: optional i64 conditionalNumGoroutinesLessThan
	8: optional bool conditionalIsOverloaded
}

struct DebugProfileStartResult {
}

struct DebugProfileStopRequest {
	1: required string name
}

struct DebugProfileStopResult {
}

struct DebugIndexMemorySegmentsRequest {
	1: required string directory
}

struct DebugIndexMemorySegmentsResult {
}
