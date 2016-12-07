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
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/topology"
)

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
