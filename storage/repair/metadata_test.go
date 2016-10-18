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
	"github.com/m3db/m3db/ts"

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

	block, exists := blocks[now]
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
	block, exists := blocks[now]
	require.True(t, exists)
	require.Equal(t, now, block.Start())

	// Add the same block and check we don't add new blocks
	m.GetOrAdd(now, testHostBlockMetadataSlicePool())
	require.Equal(t, 1, len(m.Blocks()))
}

func TestReplicaSeriesMetadataGetOrAdd(t *testing.T) {
	m := NewReplicaSeriesMetadata()

	// Add a series
	m.GetOrAdd(ts.StringID("foo"))
	series := m.Series()
	require.Equal(t, 1, len(series))
	_, exists := series[ts.StringID("foo").Hash()]
	require.True(t, exists)

	// Add the same series and check we don't add new series
	m.GetOrAdd(ts.StringID("foo"))
	require.Equal(t, 1, len(m.Series()))
}

type testBlock struct {
	id     ts.ID
	ts     time.Time
	blocks []HostBlockMetadata
}

func assertEqual(t *testing.T, expected []testBlock, actual ReplicaSeriesMetadata) {
	require.Equal(t, len(expected), int(actual.NumBlocks()))

	for _, b := range expected {
		series := actual.Series()[b.id.Hash()]
		blocks := series.Metadata.Blocks()[b.ts]
		require.Equal(t, b.blocks, blocks.Metadata())
	}
}

func TestReplicaMetadataComparerAddLocalMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := topology.NewHost("foo", "addrFoo")
	now := time.Now()
	localIter := block.NewMockFilteredBlocksMetadataIter(ctrl)
	inputBlocks := []struct {
		id   ts.ID
		meta block.Metadata
	}{
		{ts.StringID("foo"), block.NewMetadata(now, int64(0), new(uint32))},
		{ts.StringID("foo"), block.NewMetadata(now.Add(time.Second), int64(2), new(uint32))},
		{ts.StringID("bar"), block.NewMetadata(now, int64(4), nil)},
	}

	gomock.InOrder(
		localIter.EXPECT().Next().Return(true),
		localIter.EXPECT().Current().Return(inputBlocks[0].id, inputBlocks[0].meta),
		localIter.EXPECT().Next().Return(true),
		localIter.EXPECT().Current().Return(inputBlocks[1].id, inputBlocks[1].meta),
		localIter.EXPECT().Next().Return(true),
		localIter.EXPECT().Current().Return(inputBlocks[2].id, inputBlocks[2].meta),
		localIter.EXPECT().Next().Return(false),
	)

	m := NewReplicaMetadataComparer(3, testRepairOptions()).(replicaMetadataComparer)
	m.AddLocalMetadata(origin, localIter)

	expected := []testBlock{
		{inputBlocks[0].id, inputBlocks[0].meta.Start, []HostBlockMetadata{{origin, inputBlocks[0].meta.Size, inputBlocks[0].meta.Checksum}}},
		{inputBlocks[1].id, inputBlocks[1].meta.Start, []HostBlockMetadata{{origin, inputBlocks[1].meta.Size, inputBlocks[1].meta.Checksum}}},
		{inputBlocks[2].id, inputBlocks[2].meta.Start, []HostBlockMetadata{{origin, inputBlocks[2].meta.Size, inputBlocks[2].meta.Checksum}}},
	}
	assertEqual(t, expected, m.metadata)
}

func TestReplicaMetadataComparerAddPeerMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	peerIter := client.NewMockPeerBlocksMetadataIter(ctrl)
	inputBlocks := []struct {
		host topology.Host
		meta block.BlocksMetadata
	}{
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata(ts.StringID("foo"), []block.Metadata{block.NewMetadata(now, int64(0), new(uint32))})},
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata(ts.StringID("foo"), []block.Metadata{block.NewMetadata(now.Add(time.Second), int64(1), new(uint32))})},
		{topology.NewHost("2", "addr2"), block.NewBlocksMetadata(ts.StringID("foo"), []block.Metadata{block.NewMetadata(now, int64(2), nil)})},
		{topology.NewHost("2", "addr2"), block.NewBlocksMetadata(ts.StringID("bar"), []block.Metadata{block.NewMetadata(now.Add(time.Second), int64(3), nil)})},
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
		{ts.StringID("foo"), inputBlocks[0].meta.Blocks[0].Start, []HostBlockMetadata{
			{inputBlocks[0].host, inputBlocks[0].meta.Blocks[0].Size, inputBlocks[0].meta.Blocks[0].Checksum},
			{inputBlocks[2].host, inputBlocks[2].meta.Blocks[0].Size, inputBlocks[2].meta.Blocks[0].Checksum},
		}},
		{ts.StringID("foo"), inputBlocks[1].meta.Blocks[0].Start, []HostBlockMetadata{
			{inputBlocks[1].host, inputBlocks[1].meta.Blocks[0].Size, inputBlocks[1].meta.Blocks[0].Checksum},
		}},
		{ts.StringID("bar"), inputBlocks[3].meta.Blocks[0].Start, []HostBlockMetadata{
			{inputBlocks[3].host, inputBlocks[3].meta.Blocks[0].Size, inputBlocks[3].meta.Blocks[0].Checksum},
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
		metadata.GetOrAdd(ts.StringID(input.id)).GetOrAdd(input.ts, testHostBlockMetadataSlicePool()).Add(HostBlockMetadata{
			Host:     input.host,
			Size:     input.size,
			Checksum: checkSum,
		})
	}

	sizeExpected := []testBlock{
		{ts.StringID("bar"), now.Add(time.Second), []HostBlockMetadata{
			{hosts[0], int64(0), &inputs[2].checksum},
			{hosts[1], int64(1), &inputs[3].checksum},
		}},
		{ts.StringID("gah"), now.Add(3 * time.Second), []HostBlockMetadata{
			{hosts[0], int64(1), &inputs[6].checksum},
		}},
	}

	checksumExpected := []testBlock{
		{ts.StringID("baz"), now.Add(2 * time.Second), []HostBlockMetadata{
			{hosts[0], int64(2), &inputs[4].checksum},
			{hosts[1], int64(2), nil},
		}},
		{ts.StringID("gah"), now.Add(3 * time.Second), []HostBlockMetadata{
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
