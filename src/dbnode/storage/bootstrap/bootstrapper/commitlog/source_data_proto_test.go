// Copyright (c) 2019 Uber Technologies, Inc.
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

package commitlog

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

var (
	testNamespace     = ident.StringID("commitlog_prototest_ns")
	testSchemaHistory = prototest.NewSchemaHistory()
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testSchemaDesc    = namespace.GetTestSchemaDescr(testSchema)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
	testProtoEqual    = func(expect, actual []byte) bool {
		return prototest.ProtoEqual(testSchema, expect, actual)
	}
)

func testProtoOptions() Options {
	resultOpts := testDefaultOpts.ResultOptions()
	bOpts := resultOpts.DatabaseBlockOptions().
		SetEncoderPool(prototest.ProtoPools.EncoderPool).
		SetReaderIteratorPool(prototest.ProtoPools.ReaderIterPool).
		SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool)
	return testDefaultOpts.SetResultOptions(resultOpts.SetDatabaseBlockOptions(bOpts))
}

func testProtoNsMetadata(t *testing.T) namespace.Metadata {
	nOpts := namespace.NewOptions().SetSchemaHistory(testSchemaHistory)
	md, err := namespace.NewMetadata(testNamespace, nOpts)
	require.NoError(t, err)
	return md
}

func setProtoAnnotation(value testValues) testValues {
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)
	for i := 0; i < len(value); i++ {
		value[i].v = 0
		value[i].a = protoIter.Next()
	}
	return value
}

func TestProtoReadOrderedValues(t *testing.T) {
	opts := testProtoOptions()
	md := testProtoNsMetadata(t)
	testReadOrderedValues(t, opts, md, setProtoAnnotation)
}

func TestProtoReadUnorderedValues(t *testing.T) {
	opts := testProtoOptions()
	md := testProtoNsMetadata(t)
	testReadUnorderedValues(t, opts, md, setProtoAnnotation)
}

func TestProtoItMergesSnapshotsAndCommitLogs(t *testing.T) {
	opts := testProtoOptions()
	md := testProtoNsMetadata(t)
	testItMergesSnapshotsAndCommitLogs(t, opts, md, setProtoAnnotation)
}
