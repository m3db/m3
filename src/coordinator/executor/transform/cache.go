// Copyright (c) 2018 Uber Technologies, Inc.
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

package transform

import (
	"sync"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/parser"
)

// BlockCache is used to cache blocks
type BlockCache struct {
	blocks map[parser.NodeID]block.Block
	mu     sync.Mutex
}

// NewBlockCache creates a new BlockCache
func NewBlockCache() *BlockCache {
	return &BlockCache{
		blocks: make(map[parser.NodeID]block.Block),
	}
}

// Add the block to the cache
func (c *BlockCache) Add(key parser.NodeID, b block.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[key] = b
}

// Remove the block from the cache
func (c *BlockCache) Remove(key parser.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.blocks, key)
}

// Get the block from the cache
func (c *BlockCache) Get(key parser.NodeID) (block.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blocks[key]
	return b, ok
}
