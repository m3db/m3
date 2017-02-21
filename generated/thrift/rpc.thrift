// Copyright (c) 2016 Uber Technologies, Inc.
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

exception Error {
	1: required ErrorType type = ErrorType.INTERNAL_ERROR
	2: required string message
}

exception WriteBatchRawErrors {
	1: required list<WriteBatchRawError> errors
}

service Node {
	// Friendly not highly performant read/write endpoints
	FetchResult fetch(1: FetchRequest req) throws (1: Error err)
	void write(1: WriteRequest req) throws (1: Error err)

	// Performant read/write endpoints
	FetchBatchRawResult fetchBatchRaw(1: FetchBatchRawRequest req) throws (1: Error err)
	FetchBlocksRawResult fetchBlocksRaw(1: FetchBlocksRawRequest req) throws (1: Error err)
	FetchBlocksMetadataRawResult fetchBlocksMetadataRaw(1: FetchBlocksMetadataRawRequest req) throws (1: Error err)
	void writeBatchRaw(1: WriteBatchRawRequest req) throws (1: WriteBatchRawErrors err)
	void repair() throws (1: Error err)
	TruncateResult truncate(1: TruncateRequest req) throws (1: Error err)

	// Management endpoints
	NodeHealthResult health() throws (1: Error err)
	NodePersistRateLimitResult getPersistRateLimit() throws (1: Error err)
	NodePersistRateLimitResult setPersistRateLimit(1: NodeSetPersistRateLimitRequest req) throws (1: Error err)
}

struct FetchRequest {
	1: required i64 rangeStart
	2: required i64 rangeEnd
	3: required string nameSpace
	4: required string id
	5: optional TimeType rangeType = TimeType.UNIX_SECONDS
	6: optional TimeType resultTimeType = TimeType.UNIX_SECONDS
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

struct WriteRequest {
	1: required string nameSpace
	2: required string id
	3: required Datapoint datapoint
}

struct FetchBatchRawRequest {
	1: required i64 rangeStart
	2: required i64 rangeEnd
	3: required binary nameSpace
	4: required list<binary> ids
	5: optional TimeType rangeType = TimeType.UNIX_SECONDS
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
}

struct FetchBlocksRawRequest {
	1: required binary nameSpace
	2: required i32 shard
	3: required list<FetchBlocksRawRequestElement> elements
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

struct FetchBlocksMetadataRawRequest {
	1: required binary nameSpace
	2: required i32 shard
	3: required i64 rangeStart
	4: required i64 rangeEnd
	5: required i64 limit
	6: optional i64 pageToken
	7: optional bool includeSizes
	8: optional bool includeChecksums
}

struct FetchBlocksMetadataRawResult {
	1: required list<BlocksMetadata> elements
	2: optional i64 nextPageToken
}

struct BlocksMetadata {
	1: required binary id
	2: required list<BlockMetadata> blocks
}

struct BlockMetadata {
	1: optional Error err
	2: required i64 start
	3: optional i64 size
	4: optional i64 checksum
}

struct WriteBatchRawRequest {
	1: required binary nameSpace
	2: required list<WriteBatchRawRequestElement> elements
}

struct WriteBatchRawRequestElement {
	1: required binary id
	2: required Datapoint datapoint
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
}

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

service Cluster {
	HealthResult health() throws (1: Error err)
	void write(1: WriteRequest req) throws (1: Error err)
	FetchResult fetch(1: FetchRequest req) throws (1: Error err)
	TruncateResult truncate(1: TruncateRequest req) throws (1: Error err)
}

struct HealthResult {
	1: required bool ok
	2: required string status
}
