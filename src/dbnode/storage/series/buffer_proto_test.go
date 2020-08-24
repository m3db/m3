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

	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/x/ident"

	"github.com/m3db/m3/src/dbnode/namespace"
)

var (
	testNamespace     = ident.StringID("buffer_test_ns")
	testSchemaHistory = prototest.NewSchemaHistory()
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testSchemaDesc    = namespace.GetTestSchemaDescr(testSchema)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
	testProtoEqual    = func(t *testing.T, expect, actual []byte) {
		prototest.RequireEqual(t, testSchema, expect, actual)
	}
)

func newBufferTestProtoOptions(t *testing.T) Options {
	bufferBucketPool := NewBufferBucketPool(nil)
	bufferBucketVersionsPool := NewBufferBucketVersionsPool(nil)

	opts := NewOptions().
		SetEncoderPool(prototest.ProtoPools.EncoderPool).
		SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool).
		SetBufferBucketPool(bufferBucketPool).
		SetBufferBucketVersionsPool(bufferBucketVersionsPool).
		SetRuntimeOptionsManager(m3dbruntime.NewOptionsManager())
	opts = opts.
		SetRetentionOptions(opts.RetentionOptions().
			SetBlockSize(2 * time.Minute).
			SetBufferFuture(10 * time.Second).
			SetBufferPast(10 * time.Second)).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions().
			SetContextPool(opts.ContextPool()).
			SetEncoderPool(opts.EncoderPool()).
			SetMultiReaderIteratorPool(opts.MultiReaderIteratorPool()))

	return opts
}

func testSetProtoAnnotation(data []DecodedTestValue) []DecodedTestValue {
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)
	for i := 0; i < len(data); i++ {
		data[i].Value = 0
		data[i].Annotation = protoIter.Next()
	}
	return data
}

func TestProtoBufferWriteRead(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferWriteRead(t, opts, testSetProtoAnnotation)
}

func TestProtoBufferToStream(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBuffertoStream(t, opts, testSetProtoAnnotation)
}

func TestProtoBufferBucketMerge(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferBucketMerge(t, opts, testSetProtoAnnotation)
}

func TestProtoBufferSnapshot(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferSnapshot(t, opts, testSetProtoAnnotation)
}

func TestProtoBufferFetchBlocks(t *testing.T) {
	opts := newBufferTestProtoOptions(t)
	testBufferFetchBlocks(t, opts, testSetProtoAnnotation)
}
