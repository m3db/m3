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
)

type indexChecksumBlockReader struct {
	mu     sync.Mutex
	closed bool

	currentBlock IndexChecksumBlockBatch
	blocks       chan IndexChecksumBlockBatch
}

// NewIndexChecksumBlockBatchReader creates a new IndexChecksumBlockBatchReader.
func NewIndexChecksumBlockBatchReader(
	blockInput chan IndexChecksumBlockBatch,
) IndexChecksumBlockBatchReader {
	return &indexChecksumBlockReader{
		blocks: blockInput,
	}
}

func (b *indexChecksumBlockReader) Current() IndexChecksumBlockBatch {
	return b.currentBlock
}

func (b *indexChecksumBlockReader) Next() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return false
	}

	if bl, ok := <-b.blocks; ok {
		b.currentBlock = bl
		return true
	}

	b.closed = true
	return false
}

func (b *indexChecksumBlockReader) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	// NB: drain block channel.
	for range b.blocks {
	}

	b.closed = true
	return
}
