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

package client

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHostBlocks struct {
	host   topology.Host
	blocks block.BlocksMetadata
}

func TestPeerBlocksMetadataIter(t *testing.T) {
	var (
		inputCh = make(chan blocksMetadata)
		errCh   = make(chan error, 1)
		err     = errors.New("foo")
	)

	opts := newHostQueueTestOptions()
	peer := newHostQueue(h, nil, nil, opts)
	now := time.Now()
	checksums := []uint32{1, 2, 3}
	inputs := []blocksMetadata{
		{peer: peer, id: ts.StringID("foo"), blocks: []blockMetadata{
			{start: now, size: int64(1), checksum: &checksums[0]},
			{start: now.Add(time.Second), size: int64(2), checksum: &checksums[1]},
		}},
		{peer: peer, id: ts.StringID("bar"), blocks: []blockMetadata{
			{start: now, size: int64(3), checksum: &checksums[2]},
		}},
		{peer: peer, id: ts.StringID("baz"), blocks: []blockMetadata{
			{start: now, size: int64(4), checksum: nil},
		}},
	}

	go func() {
		for _, input := range inputs {
			inputCh <- input
		}
		errCh <- err
		close(inputCh)
	}()

	var actual []testHostBlocks
	it := newMetadataIter(inputCh, errCh)
	for it.Next() {
		host, blocks := it.Current()
		var m []block.Metadata
		for _, b := range blocks.Blocks {
			m = append(m, block.NewMetadata(b.Start, b.Size, b.Checksum))
		}
		actualBlocks := block.NewBlocksMetadata(blocks.ID, m)
		actual = append(actual, testHostBlocks{host, actualBlocks})
	}

	expected := []testHostBlocks{
		{h, block.NewBlocksMetadata(ts.StringID("foo"), []block.Metadata{
			block.NewMetadata(inputs[0].blocks[0].start, inputs[0].blocks[0].size, inputs[0].blocks[0].checksum),
			block.NewMetadata(inputs[0].blocks[1].start, inputs[0].blocks[1].size, inputs[0].blocks[1].checksum),
		})},
		{h, block.NewBlocksMetadata(ts.StringID("bar"), []block.Metadata{
			block.NewMetadata(inputs[1].blocks[0].start, inputs[1].blocks[0].size, inputs[1].blocks[0].checksum),
		})},
		{h, block.NewBlocksMetadata(ts.StringID("baz"), []block.Metadata{
			block.NewMetadata(inputs[2].blocks[0].start, inputs[2].blocks[0].size, inputs[2].blocks[0].checksum),
		})},
	}

	require.Equal(t, len(expected), len(actual))
	for i, expected := range expected {
		actual := actual[i]
		assert.Equal(t, expected.host.String(), actual.host.String())
		assert.True(t, expected.blocks.ID.Equal(actual.blocks.ID))
		require.Equal(t, len(expected.blocks.Blocks), len(actual.blocks.Blocks))
		for j, expected := range expected.blocks.Blocks {
			actual := actual.blocks.Blocks[j]
			assert.Equal(t, expected, actual)
		}
	}
}
