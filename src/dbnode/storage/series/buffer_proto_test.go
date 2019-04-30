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

	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

var (
	testNamespace     = ident.StringID("buffer_test_ns")
	// Relative to $pwd to testdata/prototest, $pwd is currnet directory.
	testSchemaHistory = prototest.NewSchemaHistory("../../")
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

func testSetProtoAnnotation(data []value) []value {
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)
	for i := 0; i < len(data); i++ {
		data[i].value = 0
		data[i].annotation = protoIter.Next()
	}
	return data
}

func TestBufferProtoWriteRead(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferWriteRead(t, opts, testSetProtoAnnotation, testProtoEqual)
}

func TestBufferProtoToStream(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBuffertoStream(t, opts, testSetProtoAnnotation, testProtoEqual)
}

func TestBufferBucketProtoMerge(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferBucketMerge(t, opts, testSetProtoAnnotation, testProtoEqual)
}

func TestBufferProtoSnapshot(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferSnapshot(t, opts, testSetProtoAnnotation, testProtoEqual)
}
