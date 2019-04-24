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

package series

import (
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/dbnode/storage/series/testdata"
)

var (
	testNamespace = ident.StringID("buffer_test_ns")
)

func newBufferTestProtoOptions(t *testing.T) Options {
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	testEncodingOptions := encoding.NewOptions().
		SetDefaultTimeUnit(xtime.Second).
		SetBytesPool(bytesPool)

	encoderPool := encoding.NewEncoderPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)

	encodingOpts := testEncodingOptions.SetEncoderPool(encoderPool)

	encoderPool.Init(func() encoding.Encoder {
		return proto.NewEncoder(timeZero, encodingOpts)
	})
	multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return proto.NewIterator(r, encodingOpts)
	})

	bufferBucketPool := NewBufferBucketPool(nil)
	bufferBucketVersionsPool := NewBufferBucketVersionsPool(nil)

	opts := NewOptions().
		SetEncoderPool(encoderPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool).
		SetBufferBucketPool(bufferBucketPool).
		SetBufferBucketVersionsPool(bufferBucketVersionsPool)
	opts = opts.
		SetRetentionOptions(opts.RetentionOptions().
		SetBlockSize(2 * time.Minute).
		SetBufferFuture(10 * time.Second).
		SetBufferPast(10 * time.Second)).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions().
		SetContextPool(opts.ContextPool()).
		SetEncoderPool(opts.EncoderPool()).
		SetMultiReaderIteratorPool(opts.MultiReaderIteratorPool())).
		SetNamespaceId(testNamespace)

	err := opts.SchemaRegistry().SetSchemaHistory(testNamespace, testdata.TestSchemaHistory)
	require.NoError(t, err)

	return opts
}

func TestBufferProtoWriteRead(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)

	count := len(testdata.TestProtoMessages)
	data := make([]value, count)
	for i := 0; i < count; i++ {
		currTime := curr.Add(time.Duration(i) * time.Second)
		testAnn, err := testdata.TestProtoMessages[i].Marshal()
		require.NoError(t, err)
		data[i] = value{currTime, 0, xtime.Second, testAnn}
	}

	for _, v := range data {
		verifyWriteToBuffer(t, buffer, v)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts, testdata.RequireEqual)
}
