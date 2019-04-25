package block

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/testdata/prototest"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testNamespace     = ident.StringID("block_test_ns")
	testSchemaHistory = prototest.NewSchemaHistory("../testdata/prototest")
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
	curr := time.Now()
	data := []ts.Datapoint{
		{
			Timestamp: curr,
			Value:     0,
		},
		{
			Timestamp: curr.Add(time.Second),
			Value:     0,
		},
	}
	durations := []time.Duration{
		time.Minute,
		time.Hour,
	}

	testNamespaceCtx := namespace.Context{
		Id:     testNamespace,
		Schema: testSchemaDesc,
	}

	// Setup encoding pools.
	blockOpts := NewOptions().
		SetReaderIteratorPool(prototest.ProtoPools.ReaderIterPool).
		SetEncoderPool(prototest.ProtoPools.EncoderPool).
		SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool)

	// Create the two blocks we plan to merge
	encoder := blockOpts.EncoderPool().Get()

	encoder.SetSchema(testSchemaDesc)
	encoder.Reset(data[0].Timestamp, 10)
	encoder.Encode(data[0], xtime.Second, piter.Next())
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].Timestamp, durations[0], seg, blockOpts).(*dbBlock)
	block1.SetNamespaceContext(testNamespaceCtx)

	encoder.SetSchema(testSchemaDesc)
	encoder.Reset(data[1].Timestamp, 10)
	encoder.Encode(data[1], xtime.Second, piter.Next())
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].Timestamp, durations[1], seg, blockOpts).(*dbBlock)
	block2.SetNamespaceContext(testNamespaceCtx)

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
	iter.SetSchema(testSchemaDesc)
	iter.Reset(reader)

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
	require.Equal(t, digest.SegmentChecksum(seg), mergedChecksum)

	depCtx.BlockingClose()
	block1.Close()
	block2.Close()
}
