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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/testdata/prototest"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testNamespace     = ident.StringID("buffer_test_ns")
	testSchemaHistory = prototest.NewSchemaHistory("../testdata/prototest")
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
	testProtoEqual    = func(t *testing.T, expect, actual []byte) {
		prototest.RequireEqual(t, testSchema, expect, actual)}
)

func newBufferTestProtoOptions(t *testing.T) Options {
	bufferBucketPool := NewBufferBucketPool(nil)
	bufferBucketVersionsPool := NewBufferBucketVersionsPool(nil)

	opts := NewOptions().
		SetEncoderPool(prototest.ProtoPools.EncoderPool).
		SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool).
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

	err := opts.SchemaRegistry().SetSchemaHistory(testNamespace, testSchemaHistory)
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

	count := len(testProtoMessages)
	data := make([]value, count)
	for i := 0; i < count; i++ {
		currTime := curr.Add(time.Duration(i) * time.Second)
		data[i] = value{currTime, 0, xtime.Second, testProtoMessages[i]}
	}

	testBufferWriteRead(t, data, opts, testProtoEqual)
}

func newBucketsProtoFixture(t *testing.T) ([][]value, Options) {
	opts := newBufferTestProtoOptions(t)
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	iter := prototest.NewProtoMessageIterator(testProtoMessages)
	data := [][]value{
		{
			{curr, 0, xtime.Second, iter.Next()},
			{curr.Add(secs(10)), 0, xtime.Second, iter.Next()},
			{curr.Add(secs(50)), 0, xtime.Second, iter.Next()},
		},
		{
			{curr.Add(secs(20)), 0, xtime.Second, iter.Next()},
			{curr.Add(secs(40)), 0, xtime.Second, iter.Next()},
			{curr.Add(secs(60)), 0, xtime.Second, iter.Next()},
		},
		{
			{curr.Add(secs(30)), 0, xtime.Second, iter.Next()},
			{curr.Add(secs(70)), 0, xtime.Second, iter.Next()},
		},
		{
			{curr.Add(secs(35)), 0, xtime.Second, iter.Next()},
		},
	}
	return data, opts
}

func TestBufferProtoToStream(t *testing.T) {
	data, opts := newBucketsProtoFixture(t)
	testBuffertoStream(t, data, opts, testProtoEqual)
}

func TestBufferBucketProtoMerge(t *testing.T) {
	data, opts := newBucketsProtoFixture(t)
	testBufferBucketMerge(t, data, opts, testProtoEqual)
}

func TestBufferProtoSnapshot(t *testing.T) {
	var (
		opts      = newBufferTestProtoOptions(t)
		rops      = opts.RetentionOptions()
		blockSize = rops.BlockSize()
		curr      = time.Now().Truncate(blockSize)
	)

	iter := prototest.NewProtoMessageIterator(testProtoMessages)
	// Create test data to perform out of order writes that will create two in-order
	// encoders so we can verify that Snapshot will perform a merge
	data := []value{
		{curr, 0, xtime.Second, iter.Next()},
		{curr.Add(mins(0.5)), 0, xtime.Second, iter.Next()},
		{curr.Add(mins(0.5)).Add(-5 * time.Second), 0, xtime.Second, iter.Next()},
		{curr.Add(mins(1.0)), 0, xtime.Second, iter.Next()},
		{curr.Add(mins(1.5)), 0, xtime.Second, iter.Next()},
		{curr.Add(mins(1.5)).Add(-5 * time.Second), 0, xtime.Second, iter.Next()},

		// Add one write for a different block to make sure Snapshot only returns
		// date for the requested block
		{curr.Add(blockSize), 6, xtime.Second, nil},
	}
	testBufferSnapshot(t, data, opts, testProtoEqual)
}
