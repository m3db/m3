// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package wide

import (
	"sync"

	"github.com/m3db/m3/src/x/ident"
)

type indexChecksumBlockBuffer struct {
	sync.RWMutex
	closed       bool
	currentBlock ident.IndexChecksumBlock
	blocks       chan ident.IndexChecksumBlock
	buffer       []ident.IndexChecksumBlock
}

// NewIndexChecksumBlockBuffer creates a new IndexChecksumBlockBuffer.
func NewIndexChecksumBlockBuffer() IndexChecksumBlockBuffer {
	return &indexChecksumBlockBuffer{
		blocks: make(chan ident.IndexChecksumBlock),
		buffer: make([]ident.IndexChecksumBlock, 0, 10),
	}
}

func (b *indexChecksumBlockBuffer) Close() {
	b.Lock()
	if b.closed {
		b.Unlock()
		return
	}

	b.closed = true
	b.Unlock()

	close(b.blocks)
}

func (b *indexChecksumBlockBuffer) Push(bl ident.IndexChecksumBlock) {
	b.RLock()
	if b.closed {
		b.RUnlock()
		return
	}

	b.RUnlock()
	b.blocks <- bl
}

func (b *indexChecksumBlockBuffer) Current() ident.IndexChecksumBlock {
	return b.currentBlock
}

func (b *indexChecksumBlockBuffer) Next() bool {
	b.RLock()
	if b.closed {
		b.RUnlock()
		return false
	}

	b.RUnlock()
	if bl, ok := <-b.blocks; ok {
		b.currentBlock = bl
		return true
	}

	return false
}

func (b *indexChecksumBlockBuffer) DrainAndClose() {
	for range b.blocks {
	}

	b.Close()
}
