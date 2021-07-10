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

package repair

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testReplicaMetadataSlicePool() ReplicaMetadataSlicePool {
	return NewReplicaMetadataSlicePool(nil, 0)
}

func testRepairOptions() Options {
	return NewOptions()
}

func TestReplicaBlockMetadataAdd(t *testing.T) {
	meta1 := block.NewMetadata(
		ident.StringID("some-id"), ident.Tags{}, 0, 1, nil, 0)
	meta2 := block.NewMetadata(
		ident.StringID("some-id"), ident.Tags{}, 0, 2, new(uint32), 0)

	now := xtime.Now()
	m := NewReplicaBlockMetadata(now, newReplicaMetadataSlice())
	inputs := []block.ReplicaMetadata{
		{Host: topology.NewHost("foo", "addrFoo"), Metadata: meta1},
		{Host: topology.NewHost("bar", "addrBar"), Metadata: meta2},
	}
	for _, input := range inputs {
		m.Add(input)
	}
	require.Equal(t, now, m.Start())
	require.Equal(t, inputs, m.Metadata())
}

func TestReplicaBlocksMetadataAdd(t *testing.T) {
	now := xtime.Now()
	block := NewReplicaBlockMetadata(now, newReplicaMetadataSlice())
	m := NewReplicaBlocksMetadata()
	m.Add(block)

	blocks := m.Blocks()
	require.Equal(t, 1, len(blocks))

	block, exists := blocks[now]
	require.True(t, exists)
	require.Equal(t, now, block.Start())
}

func TestReplicaBlocksMetadataGetOrAdd(t *testing.T) {
	now := xtime.Now()
	m := NewReplicaBlocksMetadata()
	require.Equal(t, 0, len(m.Blocks()))

	// Add a block
	b := m.GetOrAdd(now, testReplicaMetadataSlicePool())
	require.Equal(t, now, b.Start())
	blocks := m.Blocks()
	require.Equal(t, 1, len(blocks))
	block, exists := blocks[now]
	require.True(t, exists)
	require.Equal(t, now, block.Start())

	// Add the same block and check we don't add new blocks
	m.GetOrAdd(now, testReplicaMetadataSlicePool())
	require.Equal(t, 1, len(m.Blocks()))
}

func TestReplicaSeriesMetadataGetOrAdd(t *testing.T) {
	m := NewReplicaSeriesMetadata()

	// Add a series
	m.GetOrAdd(ident.StringID("foo"))
	series := m.Series()
	require.Equal(t, 1, series.Len())
	_, exists := series.Get(ident.StringID("foo"))
	require.True(t, exists)

	// Add the same series and check we don't add new series
	m.GetOrAdd(ident.StringID("foo"))
	require.Equal(t, 1, m.Series().Len())
}

type testBlock struct {
	id     ident.ID
	ts     xtime.UnixNano
	blocks []block.ReplicaMetadata
}

func assertEqual(t *testing.T, expected []testBlock, actual ReplicaSeriesMetadata) {
	require.Equal(t, len(expected), int(actual.NumBlocks()))

	for _, b := range expected {
		series, ok := actual.Series().Get(b.id)
		require.True(t, ok)
		blocks := series.Metadata.Blocks()[b.ts]
		require.Equal(t, b.blocks, blocks.Metadata())
	}
}

func TestReplicaMetadataComparerAddLocalMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := topology.NewHost("foo", "addrFoo")
	now := xtime.Now()
	localIter := block.NewMockFilteredBlocksMetadataIter(ctrl)
	inputBlocks := []block.Metadata{
		block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now, int64(0), new(uint32), 0),
		block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(time.Second), int64(2), new(uint32), 0),
		block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now, int64(4), nil, 0),
	}

	gomock.InOrder(
		localIter.EXPECT().Next().Return(true),
		localIter.EXPECT().Current().Return(inputBlocks[0].ID, inputBlocks[0]),
		localIter.EXPECT().Next().Return(true),
		localIter.EXPECT().Current().Return(inputBlocks[1].ID, inputBlocks[1]),
		localIter.EXPECT().Next().Return(true),
		localIter.EXPECT().Current().Return(inputBlocks[2].ID, inputBlocks[2]),
		localIter.EXPECT().Next().Return(false),
		localIter.EXPECT().Err().Return(nil),
	)

	m := NewReplicaMetadataComparer(origin, testRepairOptions()).(replicaMetadataComparer)
	err := m.AddLocalMetadata(localIter)
	require.NoError(t, err)

	expected := []testBlock{
		{inputBlocks[0].ID, inputBlocks[0].Start, []block.ReplicaMetadata{{Host: origin, Metadata: inputBlocks[0]}}},
		{inputBlocks[1].ID, inputBlocks[1].Start, []block.ReplicaMetadata{{Host: origin, Metadata: inputBlocks[1]}}},
		{inputBlocks[2].ID, inputBlocks[2].Start, []block.ReplicaMetadata{{Host: origin, Metadata: inputBlocks[2]}}},
	}
	assertEqual(t, expected, m.metadata)
}

func TestReplicaMetadataComparerAddPeerMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := xtime.Now()
	peerIter := client.NewMockPeerBlockMetadataIter(ctrl)
	inputBlocks := []block.ReplicaMetadata{
		{
			Host: topology.NewHost("1", "addr1"),
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{},
				now, int64(0), new(uint32), 0),
		},
		{
			Host: topology.NewHost("1", "addr1"),
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{},
				now.Add(time.Second), int64(1), new(uint32), 0),
		},
		{
			Host: topology.NewHost("2", "addr2"),
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{},
				now, int64(2), nil, 0),
		},
		{
			Host: topology.NewHost("2", "addr2"),
			Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{},
				now.Add(time.Second), int64(3), nil, 0),
		},
	}
	expectedErr := errors.New("some error")

	gomock.InOrder(
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[0].Host, inputBlocks[0].Metadata),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[1].Host, inputBlocks[1].Metadata),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[2].Host, inputBlocks[2].Metadata),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[3].Host, inputBlocks[3].Metadata),
		peerIter.EXPECT().Next().Return(false),
		peerIter.EXPECT().Err().Return(expectedErr),
	)

	m := NewReplicaMetadataComparer(inputBlocks[0].Host, testRepairOptions()).(replicaMetadataComparer)
	require.Equal(t, expectedErr, m.AddPeerMetadata(peerIter))

	expected := []testBlock{
		{ident.StringID("foo"), inputBlocks[0].Metadata.Start, []block.ReplicaMetadata{
			inputBlocks[0],
			inputBlocks[2],
		}},
		{ident.StringID("foo"), inputBlocks[1].Metadata.Start, []block.ReplicaMetadata{
			inputBlocks[1],
		}},
		{ident.StringID("bar"), inputBlocks[3].Metadata.Start, []block.ReplicaMetadata{
			inputBlocks[3],
		}},
	}
	assertEqual(t, expected, m.metadata)
}

func TestReplicaMetadataComparerCompare(t *testing.T) {
	var (
		now   = xtime.Now()
		hosts = []topology.Host{topology.NewHost("foo", "foo"), topology.NewHost("bar", "bar")}
	)

	metadata := NewReplicaSeriesMetadata()
	defer metadata.Close()

	ten := uint32(10)
	twenty := uint32(20)
	inputs := []block.ReplicaMetadata{
		{
			Host:     hosts[0],
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now, int64(1), &ten, 0),
		},
		{
			Host:     hosts[1],
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now, int64(1), &ten, 0),
		},
		{
			Host:     hosts[0],
			Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(time.Second), int64(0), &ten, 0),
		},
		{
			Host:     hosts[1],
			Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(time.Second), int64(1), &ten, 0),
		},
		// hosts[0] has a checksum but hosts[1] doesn't so this block will not be repaired (skipped until the next attempt) at
		// which points hosts[1] will have merged the blocks and an accurate comparison can be made.
		{
			Host:     hosts[0],
			Metadata: block.NewMetadata(ident.StringID("baz"), ident.Tags{}, now.Add(2*time.Second), int64(2), &twenty, 0),
		},
		{
			Host:     hosts[1],
			Metadata: block.NewMetadata(ident.StringID("baz"), ident.Tags{}, now.Add(2*time.Second), int64(2), nil, 0),
		},
		// hosts[0] and hosts[1] both have a checksum, but they differ, so this should trigger a checksum mismatch.
		{
			Host:     hosts[0],
			Metadata: block.NewMetadata(ident.StringID("boz"), ident.Tags{}, now.Add(2*time.Second), int64(2), &twenty, 0),
		},
		{
			Host:     hosts[1],
			Metadata: block.NewMetadata(ident.StringID("boz"), ident.Tags{}, now.Add(2*time.Second), int64(2), &ten, 0),
		},
		// Block only exists for host[1] but host[0] is the origin so should be consider a size/checksum mismatch.
		{
			Host:     hosts[1],
			Metadata: block.NewMetadata(ident.StringID("gah"), ident.Tags{}, now.Add(3*time.Second), int64(1), &ten, 0),
		},
		// Block only exists for host[0] but host[0] is also the origin so should not be considered a size/checksum mismatch
		// since the peer not the origin is missing data.
		{
			Host:     hosts[0],
			Metadata: block.NewMetadata(ident.StringID("grr"), ident.Tags{}, now.Add(3*time.Second), int64(1), &ten, 0),
		},
	}
	for _, input := range inputs {
		metadata.GetOrAdd(input.Metadata.ID).GetOrAdd(input.Metadata.Start, testReplicaMetadataSlicePool()).Add(input)
	}

	sizeExpected := []testBlock{
		{ident.StringID("bar"), now.Add(time.Second), []block.ReplicaMetadata{
			inputs[2],
			inputs[3],
		}},
		{ident.StringID("gah"), now.Add(3 * time.Second), []block.ReplicaMetadata{
			inputs[8],
		}},
	}

	checksumExpected := []testBlock{
		{ident.StringID("boz"), now.Add(2 * time.Second), []block.ReplicaMetadata{
			inputs[6],
			inputs[7],
		}},
		{ident.StringID("gah"), now.Add(3 * time.Second), []block.ReplicaMetadata{
			inputs[8],
		}},
	}

	m := NewReplicaMetadataComparer(hosts[0], testRepairOptions()).(replicaMetadataComparer)
	m.metadata = metadata

	res := m.Compare()
	require.Equal(t, int64(6), res.NumSeries)
	require.Equal(t, int64(6), res.NumBlocks)
	assertEqual(t, sizeExpected, res.SizeDifferences)
	assertEqual(t, checksumExpected, res.ChecksumDifferences)
}
