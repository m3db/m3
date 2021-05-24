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

package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
)

var (
	testNamespace     = ident.StringID("session_prototest_ns")
	testSchemaHistory = prototest.NewSchemaHistory()
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
	testProtoEqual    = func(t *testing.T, expect, actual []byte) {
		prototest.RequireEqual(t, testSchema, expect, actual)
	}
)

func setFetchProtoAnnotation(fetches []testFetch) []testFetch {
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)
	for j := 0; j < len(fetches); j++ {
		data := fetches[j].values
		for i := 0; i < len(data); i++ {
			data[i].value = 0
			data[i].annotation = protoIter.Next()
		}
	}
	return fetches
}

func setWriteProtoAnnotation(w *writeStub) {
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)
	w.value = 0
	w.annotation = protoIter.Next()
}

func TestProtoSessionFetchIDs(t *testing.T) {
	_, ok := testSchemaHistory.GetLatest()
	require.True(t, ok)
	schemaReg := namespace.NewSchemaRegistry(true, nil)
	opts := newSessionTestOptions().
		SetSchemaRegistry(schemaReg).
		SetEncodingProto(encoding.NewOptions().SetBytesPool(prototest.ProtoPools.BytesPool))
	require.NoError(t, opts.SchemaRegistry().SetSchemaHistory(testNamespace, testSchemaHistory))
	testOpts := testOptions{nsID: testNamespace,
		opts: opts, setFetchAnn: setFetchProtoAnnotation, annEqual: testProtoEqual,
		encoderPool: prototest.ProtoPools.EncoderPool}
	testSessionFetchIDs(t, testOpts)
}

func TestProtoSessionFetchIDsNoSchema(t *testing.T) {
	opts := newSessionTestOptions().
		SetEncodingProto(encoding.NewOptions().SetBytesPool(prototest.ProtoPools.BytesPool))
	testOpts := testOptions{
		nsID:        testNamespace,
		opts:        opts,
		setFetchAnn: setFetchProtoAnnotation,
		annEqual:    testProtoEqual,
		encoderPool: prototest.ProtoPools.EncoderPool,
		expectedErr: fmt.Errorf("no protobuf schema found for namespace: %s", testNamespace.String()),
	}
	testSessionFetchIDs(t, testOpts)
}

func TestProtoSeriesIteratorRoundtrip(t *testing.T) {
	start := xtime.Now().Truncate(time.Hour)
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)

	data := []testValue{
		{0, start.Add(1 * time.Second), xtime.Second, protoIter.Next()},
		{0, start.Add(2 * time.Second), xtime.Second, protoIter.Next()},
		{0, start.Add(3 * time.Second), xtime.Second, protoIter.Next()},
	}

	schemaReg := namespace.NewSchemaRegistry(true, nil)
	require.NoError(t, schemaReg.SetSchemaHistory(testNamespace, testSchemaHistory))
	nsCtx := namespace.NewContextFor(testNamespace, schemaReg)
	encoder := prototest.ProtoPools.EncoderPool.Get()
	encoder.Reset(data[0].t, 0, nsCtx.Schema)
	for _, value := range data {
		dp := ts.Datapoint{
			TimestampNanos: value.t,
			Value:          value.value,
		}
		encoder.Encode(dp, value.unit, value.annotation)
	}
	seg := encoder.Discard()
	result := []*rpc.Segments{&rpc.Segments{
		Merged: &rpc.Segment{Head: bytesIfNotNil(seg.Head), Tail: bytesIfNotNil(seg.Tail)},
	}}

	sliceReaderPool := newReaderSliceOfSlicesIteratorPool(nil)
	sliceReaderPool.Init()
	slicesIter := sliceReaderPool.Get()
	slicesIter.Reset(result)
	multiIter := prototest.ProtoPools.MultiReaderIterPool.Get()
	multiIter.ResetSliceOfSlices(slicesIter, nsCtx.Schema)

	seriesIterPool := encoding.NewSeriesIteratorPool(nil)
	seriesIterPool.Init()
	seriesIter := seriesIterPool.Get()
	seriesIter.Reset(encoding.SeriesIteratorOptions{
		ID:             ident.StringID("test_series_id"),
		Namespace:      testNamespace,
		StartInclusive: data[0].t,
		EndExclusive:   start.Add(4 * time.Second),
		Replicas:       []encoding.MultiReaderIterator{multiIter},
	})

	i := 0
	for seriesIter.Next() {
		dp, _, ann := seriesIter.Current()
		require.Equal(t, data[i].value, dp.Value)
		require.Equal(t, data[i].t, dp.TimestampNanos)
		testProtoEqual(t, data[i].annotation, ann)
		i++
	}
	require.Equal(t, 3, i)
}

func TestProtoSessionWrite(t *testing.T) {
	_, ok := testSchemaHistory.GetLatest()
	require.True(t, ok)
	opts := newSessionTestOptions().
		SetEncodingProto(encoding.NewOptions().SetBytesPool(prototest.ProtoPools.BytesPool))
	require.NoError(t, opts.SchemaRegistry().SetSchemaHistory(testNamespace, testSchemaHistory))

	testSessionWrite(t, testOptions{
		opts:        opts,
		setWriteAnn: setWriteProtoAnnotation,
		annEqual:    testProtoEqual,
	})
}
