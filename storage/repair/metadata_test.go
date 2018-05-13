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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testHostBlockMetadataSlicePool() HostBlockMetadataSlicePool {
	return NewHostBlockMetadataSlicePool(nil, 0)
}

func testRepairOptions() Options {
	return NewOptions()
}

func TestReplicaBlockMetadataAdd(t *testing.T) {
	now := time.Now()
	m := NewReplicaBlockMetadata(now, newHostBlockMetadataSlice())
	inputs := []HostBlockMetadata{
		{topology.NewHost("foo", "addrFoo"), 1, nil},
		{topology.NewHost("bar", "addrBar"), 2, new(uint32)},
	}
	for _, input := range inputs {
		m.Add(input)
	}
	require.Equal(t, now, m.Start())
	require.Equal(t, inputs, m.Metadata())
}

func TestReplicaBlocksMetadataAdd(t *testing.T) {
	now := time.Now()
	block := NewReplicaBlockMetadata(now, newHostBlockMetadataSlice())
	m := NewReplicaBlocksMetadata()
	m.Add(block)

	blocks := m.Blocks()
	require.Equal(t, 1, len(blocks))

	block, exists := blocks[xtime.ToUnixNano(now)]
	require.True(t, exists)
	require.Equal(t, now, block.Start())
}

func TestReplicaBlocksMetadataGetOrAdd(t *testing.T) {
	now := time.Now()
	m := NewReplicaBlocksMetadata()
	require.Equal(t, 0, len(m.Blocks()))

	// Add a block
	b := m.GetOrAdd(now, testHostBlockMetadataSlicePool())
	require.Equal(t, now, b.Start())
	blocks := m.Blocks()
	require.Equal(t, 1, len(blocks))
	block, exists := blocks[xtime.ToUnixNano(now)]
	require.True(t, exists)
	require.Equal(t, now, block.Start())

	// Add the same block and check we don't add new blocks
	m.GetOrAdd(now, testHostBlockMetadataSlicePool())
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
	ts     time.Time
	blocks []HostBlockMetadata
}

func assertEqual(t *testing.T, expected []testBlock, actual ReplicaSeriesMetadata) {
	require.Equal(t, len(expected), int(actual.NumBlocks()))

	for _, b := range expected {
		series, ok := actual.Series().Get(b.id)
		require.True(t, ok)
		blocks := series.Metadata.Blocks()[xtime.ToUnixNano(b.ts)]
		require.Equal(t, b.blocks, blocks.Metadata())
	}
}

func TestReplicaMetadataComparerAddLocalMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := topology.NewHost("foo", "addrFoo")
	now := time.Now()
	localIter := block.NewMockFilteredBlocksMetadataIter(ctrl)
	inputBlocks := []block.Metadata{
		block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now, int64(0), new(uint32), time.Time{}),
		block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(time.Second), int64(2), new(uint32), time.Time{}),
		block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now, int64(4), nil, time.Time{}),
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

	m := NewReplicaMetadataComparer(3, testRepairOptions()).(replicaMetadataComparer)
	err := m.AddLocalMetadata(origin, localIter)
	require.NoError(t, err)

	expected := []testBlock{
		{inputBlocks[0].ID, inputBlocks[0].Start, []HostBlockMetadata{{origin, inputBlocks[0].Size, inputBlocks[0].Checksum}}},
		{inputBlocks[1].ID, inputBlocks[1].Start, []HostBlockMetadata{{origin, inputBlocks[1].Size, inputBlocks[1].Checksum}}},
		{inputBlocks[2].ID, inputBlocks[2].Start, []HostBlockMetadata{{origin, inputBlocks[2].Size, inputBlocks[2].Checksum}}},
	}
	assertEqual(t, expected, m.metadata)
}

func TestReplicaMetadataComparerAddPeerMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	peerIter := client.NewMockPeerBlockMetadataIter(ctrl)
	inputBlocks := []struct {
		host topology.Host
		meta block.Metadata
	}{
		{
			host: topology.NewHost("1", "addr1"),
			meta: block.NewMetadata(ident.StringID("foo"), ident.Tags{},
				now, int64(0), new(uint32), time.Time{}),
		},
		{
			host: topology.NewHost("1", "addr1"),
			meta: block.NewMetadata(ident.StringID("foo"), ident.Tags{},
				now.Add(time.Second), int64(1), new(uint32), time.Time{}),
		},
		{
			host: topology.NewHost("2", "addr2"),
			meta: block.NewMetadata(ident.StringID("foo"), ident.Tags{},
				now, int64(2), nil, time.Time{}),
		},
		{
			host: topology.NewHost("2", "addr2"),
			meta: block.NewMetadata(ident.StringID("bar"), ident.Tags{},
				now.Add(time.Second), int64(3), nil, time.Time{}),
		},
	}
	expectedErr := errors.New("some error")

	gomock.InOrder(
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[0].host, inputBlocks[0].meta),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[1].host, inputBlocks[1].meta),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[2].host, inputBlocks[2].meta),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[3].host, inputBlocks[3].meta),
		peerIter.EXPECT().Next().Return(false),
		peerIter.EXPECT().Err().Return(expectedErr),
	)

	m := NewReplicaMetadataComparer(3, testRepairOptions()).(replicaMetadataComparer)
	require.Equal(t, expectedErr, m.AddPeerMetadata(peerIter))

	expected := []testBlock{
		{ident.StringID("foo"), inputBlocks[0].meta.Start, []HostBlockMetadata{
			{inputBlocks[0].host, inputBlocks[0].meta.Size, inputBlocks[0].meta.Checksum},
			{inputBlocks[2].host, inputBlocks[2].meta.Size, inputBlocks[2].meta.Checksum},
		}},
		{ident.StringID("foo"), inputBlocks[1].meta.Start, []HostBlockMetadata{
			{inputBlocks[1].host, inputBlocks[1].meta.Size, inputBlocks[1].meta.Checksum},
		}},
		{ident.StringID("bar"), inputBlocks[3].meta.Start, []HostBlockMetadata{
			{inputBlocks[3].host, inputBlocks[3].meta.Size, inputBlocks[3].meta.Checksum},
		}},
	}
	assertEqual(t, expected, m.metadata)
}

func TestReplicaMetadataComparerCompare(t *testing.T) {
	var (
		now   = time.Now()
		hosts = []topology.Host{topology.NewHost("foo", "foo"), topology.NewHost("bar", "bar")}
	)

	metadata := NewReplicaSeriesMetadata()
	defer metadata.Close()

	inputs := []struct {
		host        topology.Host
		id          string
		ts          time.Time
		size        int64
		checksum    uint32
		hasChecksum bool
	}{
		{hosts[0], "foo", now, int64(1), uint32(10), true},
		{hosts[1], "foo", now, int64(1), uint32(10), true},
		{hosts[0], "bar", now.Add(time.Second), int64(0), uint32(10), true},
		{hosts[1], "bar", now.Add(time.Second), int64(1), uint32(10), true},
		{hosts[0], "baz", now.Add(2 * time.Second), int64(2), uint32(20), true},
		{hosts[1], "baz", now.Add(2 * time.Second), int64(2), uint32(0), false},
		{hosts[0], "gah", now.Add(3 * time.Second), int64(1), uint32(10), true},
	}
	for _, input := range inputs {
		var checkSum *uint32
		if input.hasChecksum {
			ckSum := input.checksum
			checkSum = &ckSum
		}
		metadata.GetOrAdd(ident.StringID(input.id)).GetOrAdd(input.ts, testHostBlockMetadataSlicePool()).Add(HostBlockMetadata{
			Host:     input.host,
			Size:     input.size,
			Checksum: checkSum,
		})
	}

	sizeExpected := []testBlock{
		{ident.StringID("bar"), now.Add(time.Second), []HostBlockMetadata{
			{hosts[0], int64(0), &inputs[2].checksum},
			{hosts[1], int64(1), &inputs[3].checksum},
		}},
		{ident.StringID("gah"), now.Add(3 * time.Second), []HostBlockMetadata{
			{hosts[0], int64(1), &inputs[6].checksum},
		}},
	}

	checksumExpected := []testBlock{
		{ident.StringID("baz"), now.Add(2 * time.Second), []HostBlockMetadata{
			{hosts[0], int64(2), &inputs[4].checksum},
			{hosts[1], int64(2), nil},
		}},
		{ident.StringID("gah"), now.Add(3 * time.Second), []HostBlockMetadata{
			{hosts[0], int64(1), &inputs[6].checksum},
		}},
	}

	m := NewReplicaMetadataComparer(2, testRepairOptions()).(replicaMetadataComparer)
	m.metadata = metadata

	res := m.Compare()
	require.Equal(t, int64(4), res.NumSeries)
	require.Equal(t, int64(4), res.NumBlocks)
	assertEqual(t, sizeExpected, res.SizeDifferences)
	assertEqual(t, checksumExpected, res.ChecksumDifferences)
}
