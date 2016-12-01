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
	"strings"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
)

type blockMetadata struct {
	start     time.Time
	size      int64
	checksum  *uint32
	reattempt blockMetadataReattempt
}

type blockMetadataReattempt struct {
	attempt       int
	id            ts.ID
	attempted     []hostQueue
	peersMetadata []blockMetadataReattemptPeerMetadata
}

type blockMetadataReattemptPeerMetadata struct {
	peer     hostQueue
	start    time.Time
	size     int64
	checksum *uint32
}

func (b blockMetadataReattempt) hasAttemptedPeer(peer hostQueue) bool {
	for i := range b.attempted {
		if b.attempted[i] == peer {
			return true
		}
	}
	return false
}

type receivedBlocks struct {
	submitted bool
	results   []*blocksMetadata
}

type processFn func(batch []*blocksMetadata)

type blocksMetadata struct {
	peer   hostQueue
	id     ts.ID
	blocks []blockMetadata
	idx    int
}

func (b blocksMetadata) unselectedBlocks() []blockMetadata {
	if b.idx == len(b.blocks) {
		return nil
	}
	return b.blocks[b.idx:]
}

func copyBlocksMetadata(blocks []blockMetadata, i int, j int) *blocksMetadata {
	return &blocksMetadata{
		peer: blocks[i].reattempt.peersMetadata[j].peer,
		id:   blocks[i].reattempt.id,
		blocks: []blockMetadata{blockMetadata{
			start:     blocks[i].reattempt.peersMetadata[j].start,
			size:      blocks[i].reattempt.peersMetadata[j].size,
			checksum:  blocks[i].reattempt.peersMetadata[j].checksum,
			reattempt: blocks[i].reattempt,
		}},
	}
}

type blocksMetadatas []*blocksMetadata

func (arr blocksMetadatas) swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr blocksMetadatas) hasBlocksLen() int {
	count := 0
	for i := range arr {
		if arr[i] != nil && len(arr[i].blocks) > 0 {
			count++
		}
	}
	return count
}

type peerBlocksMetadataByID []*blocksMetadata

func (arr peerBlocksMetadataByID) Len() int      { return len(arr) }
func (arr peerBlocksMetadataByID) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr peerBlocksMetadataByID) Less(i, j int) bool {
	return strings.Compare(arr[i].peer.Host().ID(), arr[j].peer.Host().ID()) < 0
}

type blocksMetadataQueue struct {
	blocksMetadata *blocksMetadata
	queue          *peerBlocksQueue
}

type blocksMetadatasQueuesByOutstandingAsc []blocksMetadataQueue

func (arr blocksMetadatasQueuesByOutstandingAsc) Len() int      { return len(arr) }
func (arr blocksMetadatasQueuesByOutstandingAsc) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr blocksMetadatasQueuesByOutstandingAsc) Less(i, j int) bool {
	outstandingFirst := atomic.LoadUint64(&arr[i].queue.assigned) - atomic.LoadUint64(&arr[i].queue.completed)
	outstandingSecond := atomic.LoadUint64(&arr[j].queue.assigned) - atomic.LoadUint64(&arr[j].queue.completed)
	return outstandingFirst < outstandingSecond
}

type blockMetadatasByTime []blockMetadata

func (b blockMetadatasByTime) Len() int      { return len(b) }
func (b blockMetadatasByTime) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b blockMetadatasByTime) Less(i, j int) bool {
	return b[i].start.Before(b[j].start)
}

func newTimesByUnixNanos(values []int64) []time.Time {
	result := make([]time.Time, len(values))
	for i := range values {
		result[i] = time.Unix(0, values[i])
	}
	return result
}

func newTimesByRPCBlocks(values []*rpc.Block) []time.Time {
	result := make([]time.Time, len(values))
	for i := range values {
		result[i] = time.Unix(0, values[i].Start)
	}
	return result
}

type metadataIter struct {
	inputCh  <-chan blocksMetadata
	errCh    <-chan error
	host     topology.Host
	blocks   []block.Metadata
	metadata block.BlocksMetadata
	done     bool
	err      error
}

func newMetadataIter(inputCh <-chan blocksMetadata, errCh <-chan error) PeerBlocksMetadataIter {
	return &metadataIter{
		inputCh: inputCh,
		errCh:   errCh,
		blocks:  make([]block.Metadata, 0, blocksMetadataInitialCapacity),
	}
}

func (it *metadataIter) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	m, more := <-it.inputCh
	if !more {
		it.err = <-it.errCh
		it.done = true
		return false
	}
	it.host = m.peer.Host()
	var zeroed block.Metadata
	for i := range it.blocks {
		it.blocks[i] = zeroed
	}
	it.blocks = it.blocks[:0]
	for _, b := range m.blocks {
		bm := block.NewMetadata(b.start, b.size, b.checksum)
		it.blocks = append(it.blocks, bm)
	}
	it.metadata = block.NewBlocksMetadata(m.id, it.blocks)
	return true
}

func (it *metadataIter) Current() (topology.Host, block.BlocksMetadata) {
	return it.host, it.metadata
}

func (it *metadataIter) Err() error {
	return it.err
}
