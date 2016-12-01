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

package client

import (
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/ts"
)

func (s *session) newFetchBlocksMetadataRawRequest(
	namespace ts.ID,
	shard uint32,
	start, end time.Time,
	pageToken *int64,
) *rpc.FetchBlocksMetadataRawRequest {
	var (
		optionIncludeSizes     = true
		optionIncludeChecksums = true
	)

	req := rpc.NewFetchBlocksMetadataRawRequest()
	req.NameSpace = namespace.Data()
	req.Shard = int32(shard)
	req.RangeStart = start.UnixNano()
	req.RangeEnd = end.UnixNano()
	req.Limit = int64(s.streamBlocksBatchSize)
	req.PageToken = pageToken
	req.IncludeSizes = &optionIncludeSizes
	req.IncludeChecksums = &optionIncludeChecksums

	return req
}

func newFetchBlocksRawRequest(
	namespace ts.ID,
	shard uint32,
	batch []*blocksMetadata,
) *rpc.FetchBlocksRawRequest {
	req := rpc.NewFetchBlocksRawRequest()
	req.NameSpace = namespace.Data()
	req.Shard = int32(shard)
	req.Elements = make([]*rpc.FetchBlocksRawRequestElement, len(batch))

	return req
}
