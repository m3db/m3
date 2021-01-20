// Copyright (c) 2021  Uber Technologies, Inc.
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

package node

import (
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/serialize"

	athrift "github.com/apache/thrift/lib/go/thrift"
)

// FetchTaggedConversionPools allows users to pass a pool for conversions.
type FetchTaggedCustomPools interface {
	convert.FetchTaggedConversionPools

	// TagEncoder returns the pool for encoding tags.
	TagEncoder() serialize.TagEncoderPool

	// SegmentsArray returns the pool for segments arrays.
	SegmentsArray() SegmentsArrayPool
}

type FetchTaggedCustomRequestHandler interface {
	Handle(
		ctx context.Context,
		db storage.Database,
		req *rpc.FetchTaggedRequest,
		pools FetchTaggedCustomPools,
	) (athrift.TStruct, error)
}
