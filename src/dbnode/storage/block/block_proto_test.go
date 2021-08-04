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

package block

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testNamespace     = ident.StringID("block_test_ns")
	testSchemaHistory = prototest.NewSchemaHistory()
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testSchemaDesc    = namespace.GetTestSchemaDescr(testSchema)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
)

// TestDatabaseBlockMergeProto lazily merges two blocks and verifies that the correct
// data is returned.
func TestDatabaseBlockMergeProto(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test proto messages
	piter := prototest.NewProtoMessageIterator(testProtoMessages)

	// Test data
	curr := xtime.Now()
	data := []ts.Datapoint{
		{
			TimestampNanos: curr,
			Value:          0,
		},
		{
			TimestampNanos: curr.Add(time.Second),
			Value:          0,
		},
	}
	durations := []time.Duration{
		time.Minute,
		time.Hour,
	}

	testNamespaceCtx := namespace.Context{
		ID:     testNamespace,
		Schema: testSchemaDesc,
	}

	// Setup encoding pools.
	blockOpts := NewOptions().
		SetReaderIteratorPool(prototest.ProtoPools.ReaderIterPool).
		SetEncoderPool(prototest.ProtoPools.EncoderPool).
		SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool)

	// Create the two blocks we plan to merge
	encoder := blockOpts.EncoderPool().Get()

	encoder.Reset(data[0].TimestampNanos, 10, testSchemaDesc)
	encoder.Encode(data[0], xtime.Second, piter.Next())
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].TimestampNanos, durations[0], seg, blockOpts, testNamespaceCtx).(*dbBlock)

	encoder.Reset(data[1].TimestampNanos, 10, testSchemaDesc)
	encoder.Encode(data[1], xtime.Second, piter.Next())
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].TimestampNanos, durations[1], seg, blockOpts, testNamespaceCtx).(*dbBlock)

	// Lazily merge the two blocks
	block1.Merge(block2)

	// BlockSize should not change
	require.Equal(t, durations[0], block1.BlockSize())

	// Try and read the data back and verify it looks good
	depCtx := block1.opts.ContextPool().Get()
	stream, err := block1.Stream(depCtx)
	require.NoError(t, err)
	seg, err = stream.Segment()
	require.NoError(t, err)
	reader := xio.NewSegmentReader(seg)
	iter := blockOpts.ReaderIteratorPool().Get()
	iter.Reset(reader, testSchemaDesc)

	piter.Reset()
	i := 0
	for iter.Next() {
		dp, _, annotation := iter.Current()
		require.True(t, data[i].Equal(dp))
		prototest.RequireEqual(t, testSchema, piter.Next(), annotation)
		i++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 2, i)

	// Make sure the checksum was updated
	mergedChecksum, err := block1.Checksum()
	require.NoError(t, err)
	require.Equal(t, seg.CalculateChecksum(), mergedChecksum)

	depCtx.BlockingClose()
	block1.Close()
	block2.Close()
}
